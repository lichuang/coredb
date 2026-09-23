//! APPEND command implementation
//!
//! APPEND key value
//! Appends value to the string stored at key, creating it if absent.
//! Returns the length of the string after the append.
//!
//! Concurrency: read-modify-write is guarded by a conditional transaction
//! that pins the observed value bytes, so concurrent APPENDs cannot truncate
//! each other (docs/bug.md §1.2). Losers re-read and retry with capped,
//! jittered backoff instead of re-competing in lockstep.

use rockraft::raft::types::{TxnCondition, TxnReply, TxnReq, UpsertKV};

use crate::encoding::StringValue;
use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::{MAX_CAS_RETRIES, backoff_delay, now_ms};
use async_trait::async_trait;

#[derive(Debug, Clone, PartialEq)]
pub struct AppendParams {
  pub key: String,
  pub value: Vec<u8>,
}

impl AppendParams {
  pub fn new(key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
    Self {
      key: key.into(),
      value: value.into(),
    }
  }

  fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
    if items.len() != 3 {
      return Err(ProtocolError::WrongArgCount("APPEND"));
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::InvalidArgument("key")),
    };

    let value = match &items[2] {
      Value::BulkString(Some(data)) => data.clone(),
      Value::SimpleString(s) => s.as_bytes().to_vec(),
      _ => return Err(ProtocolError::InvalidArgument("value")),
    };

    Ok(AppendParams::new(key, value))
  }
}

/// The current string state observed for the target key.
enum CurrentValue {
  /// Key absent: append starts a fresh string, guarded by `not_exists`.
  Absent,
  /// Present, unexpired, and a string; carries its serialized bytes plus TTL.
  String { raw: Vec<u8>, expires_at: u64 },
  /// Present but expired: logically absent, yet its bytes still sit in the
  /// store, so the CAS must pin those bytes with `eq` — `not_exists` would
  /// never hold and the command would spin out its retry budget.
  Expired { raw: Vec<u8> },
  /// Present but not a string value: report WRONGTYPE instead of overwriting.
  Other,
}

async fn observe_current(server: &Server, key: &str) -> Result<CurrentValue, CoreDbError> {
  let raw = match server.get(key).await? {
    Some(bytes) => bytes,
    None => return Ok(CurrentValue::Absent),
  };

  match StringValue::deserialize(&raw) {
    Ok(sv) if sv.is_expired(now_ms()) => Ok(CurrentValue::Expired { raw }),
    Ok(sv) => Ok(CurrentValue::String {
      raw,
      expires_at: sv.expires_at,
    }),
    Err(_) => Ok(CurrentValue::Other),
  }
}

pub struct AppendCommand;

#[async_trait]
impl Command for AppendCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    let params = AppendParams::parse(items)?;

    for attempt in 0..MAX_CAS_RETRIES {
      // Pin the observed bytes of the whole stored value; the append only
      // lands if nothing else changed the key between read and apply.
      let (condition, appended) = match observe_current(server, &params.key).await? {
        CurrentValue::Other => return Err(ProtocolError::WrongType.into()),
        CurrentValue::Absent => (
          TxnCondition::not_exists(&params.key),
          StringValue::new(params.value.clone()).serialize(),
        ),
        // An expired key reads as absent, but its bytes are still stored:
        // pin them with `eq` (not_exists would never hold) and write a
        // fresh string without the old TTL.
        CurrentValue::Expired { raw } => (
          TxnCondition::eq(&params.key, raw),
          StringValue::new(params.value.clone()).serialize(),
        ),
        CurrentValue::String { raw, expires_at } => {
          let existing = StringValue::deserialize(&raw)
            .map_err(|_| ProtocolError::WrongType)?
            .data;
          let mut appended = existing.clone();
          appended.extend_from_slice(&params.value);
          (
            TxnCondition::eq(&params.key, raw),
            match expires_at {
              crate::encoding::NO_EXPIRATION => StringValue::new(appended),
              ttl => StringValue::with_expiration(appended, ttl),
            }
            .serialize(),
          )
        }
      };

      let reply = server
        .txn(
          TxnReq::new(vec![condition]).if_then_ops(vec![UpsertKV::insert(&params.key, &appended)]),
        )
        .await?;

      match reply {
        TxnReply::Success { branch: true, .. } => {
          let len = StringValue::deserialize(&appended)
            .map(|v| v.data.len() as i64)
            .unwrap_or(0);
          return Ok(Value::Integer(len));
        }
        // Lost the race; back off (jittered, capped) so concurrent losers do
        // not re-compete in lockstep, then re-observe.
        _ => {
          tokio::time::sleep(backoff_delay(attempt)).await;
          continue;
        }
      }
    }

    Err(ProtocolError::Custom("ERR append retry limit exceeded").into())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_append_params_parse_basic() {
    let items = vec![
      Value::BulkString(Some(b"APPEND".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
    ];
    let params = AppendParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
    assert_eq!(params.value, b"myvalue");
  }

  #[test]
  fn test_append_params_parse_with_simple_string() {
    let items = vec![
      Value::SimpleString("APPEND".to_string()),
      Value::SimpleString("mykey".to_string()),
      Value::SimpleString("myvalue".to_string()),
    ];
    let params = AppendParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
    assert_eq!(params.value, b"myvalue");
  }

  #[test]
  fn test_append_params_parse_empty_value() {
    let items = vec![
      Value::BulkString(Some(b"APPEND".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"".to_vec())),
    ];
    let params = AppendParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
    assert!(params.value.is_empty());
  }

  #[test]
  fn test_append_params_parse_binary_value() {
    let binary_data = vec![0x00, 0xFF, 0xAB, 0xCD];
    let items = vec![
      Value::BulkString(Some(b"APPEND".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(binary_data.clone())),
    ];
    let params = AppendParams::parse(&items).unwrap();
    assert_eq!(params.value, binary_data);
  }

  #[test]
  fn test_append_params_parse_missing_key() {
    let items = vec![Value::BulkString(Some(b"APPEND".to_vec()))];
    assert!(AppendParams::parse(&items).is_err());
  }

  #[test]
  fn test_append_params_parse_missing_value() {
    let items = vec![
      Value::BulkString(Some(b"APPEND".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
    ];
    assert!(AppendParams::parse(&items).is_err());
  }

  #[test]
  fn test_append_params_parse_too_many_args() {
    let items = vec![
      Value::BulkString(Some(b"APPEND".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
      Value::BulkString(Some(b"extra".to_vec())),
    ];
    assert!(AppendParams::parse(&items).is_err());
  }

  #[test]
  fn test_append_params_parse_invalid_key_type() {
    let items = vec![
      Value::BulkString(Some(b"APPEND".to_vec())),
      Value::Integer(123),
      Value::BulkString(Some(b"myvalue".to_vec())),
    ];
    assert!(AppendParams::parse(&items).is_err());
  }

  #[test]
  fn test_append_params_parse_invalid_value_type() {
    let items = vec![
      Value::BulkString(Some(b"APPEND".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::Integer(123),
    ];
    assert!(AppendParams::parse(&items).is_err());
  }
}
