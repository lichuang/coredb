//! HINCRBY command implementation
//!
//! HINCRBY key field increment
//! Increments the integer value of a field in a hash by a number.
//! Uses 0 as initial value if the field doesn't exist.

use rockraft::raft::types::{TxnCondition, TxnReply, TxnReq, UpsertKV};

use crate::encoding::{HashFieldValue, HashMetadata};
use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::{MAX_CAS_RETRIES, backoff_delay, now_ms};
use async_trait::async_trait;

/// Parsed HINCRBY arguments
#[derive(Debug)]
struct HIncrByArgs {
  key: String,
  field: Vec<u8>,
  increment: i64,
}

/// HINCRBY command handler
pub struct HIncrByCommand;

impl HIncrByCommand {
  /// Parse arguments from RESP items
  /// Format: HINCRBY key field increment
  fn parse_args(items: &[Value]) -> Result<HIncrByArgs, ProtocolError> {
    // HINCRBY key field increment (4 items)
    if items.len() != 4 {
      return Err(ProtocolError::WrongArgCount("hincrby"));
    }

    // Parse key
    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::InvalidArgument("key")),
    };

    // Parse field
    let field = match &items[2] {
      Value::BulkString(Some(data)) => data.clone(),
      Value::SimpleString(s) => s.as_bytes().to_vec(),
      _ => return Err(ProtocolError::InvalidArgument("field")),
    };

    // Parse increment
    let increment = match &items[3] {
      Value::BulkString(Some(data)) => {
        let s = String::from_utf8_lossy(data);
        s.parse::<i64>().map_err(|_| ProtocolError::NotAnInteger)?
      }
      Value::SimpleString(s) => s.parse::<i64>().map_err(|_| ProtocolError::NotAnInteger)?,
      Value::Integer(i) => *i,
      _ => return Err(ProtocolError::NotAnInteger),
    };

    Ok(HIncrByArgs {
      key,
      field,
      increment,
    })
  }
}

#[async_trait]
impl Command for HIncrByCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    // Parse arguments
    let args = Self::parse_args(items)?;

    let mut attempt = 0;
    for _ in 0..MAX_CAS_RETRIES {
      let raw_meta = server.get(&args.key).await?;
      let metadata = match &raw_meta {
        Some(bytes) => match HashMetadata::deserialize(bytes) {
          Ok(meta) if !meta.is_expired(now_ms()) => Some(meta),
          // Expired hashes count as absent; the CAS still pins their bytes so
          // the metadata write only lands if nothing recreated the key.
          _ => None,
        },
        None => None,
      };

      let version = metadata.as_ref().map_or_else(
        || {
          let fresh = HashMetadata::new();
          fresh.version
        },
        |meta| meta.version,
      );
      let sub_key_str =
        HashFieldValue::build_sub_key_hex(args.key.as_bytes(), version, &args.field);

      let raw_field = server.get(&sub_key_str).await?;
      let current_value = decode_current_i64(raw_field.clone())?;

      let new_value = current_value
        .checked_add(args.increment)
        .ok_or(ProtocolError::Overflow)?;

      let new_metadata = match &metadata {
        Some(meta) => {
          let mut m = meta.clone();
          if raw_field.is_none() {
            m.incr_size();
          }
          m
        }
        None => {
          let mut m = HashMetadata::new();
          m.version = version;
          m.incr_size();
          m
        }
      };

      // Pin the observed state of both keys: the increment only lands if the
      // metadata and the field value are unchanged at apply time.
      let conditions = vec![
        match &raw_meta {
          None => TxnCondition::not_exists(&args.key),
          Some(bytes) => TxnCondition::eq(&args.key, bytes),
        },
        match &raw_field {
          None => TxnCondition::not_exists(&sub_key_str),
          Some(bytes) => TxnCondition::eq(&sub_key_str, bytes),
        },
      ];

      let entries = vec![
        UpsertKV::insert(
          &sub_key_str,
          &HashFieldValue::new(new_value.to_string()).serialize(),
        ),
        UpsertKV::insert(&args.key, &new_metadata.serialize()),
      ];

      let req = TxnReq::new(conditions).if_then_ops(entries);
      match server.txn(req).await? {
        TxnReply::Success { branch: true, .. } => return Ok(Value::Integer(new_value)),
        // Lost the race; back off (jittered, capped) so concurrent losers do
        // not re-compete in lockstep, then re-observe.
        _ => {
          tokio::time::sleep(backoff_delay(attempt)).await;
          attempt += 1;
          continue;
        }
      }
    }

    Err(ProtocolError::Custom("ERR hincrby retry limit exceeded").into())
  }
}

fn decode_current_i64(raw_value: Option<Vec<u8>>) -> Result<i64, CoreDbError> {
  match raw_value {
    Some(bytes) => match HashFieldValue::deserialize(&bytes) {
      Ok(field_value) => {
        let s = String::from_utf8_lossy(&field_value.data);
        s.parse::<i64>()
          .map_err(|_| ProtocolError::Custom("ERR hash value is not an integer").into())
      }
      Err(_) => Err(ProtocolError::Custom("ERR hash value is not an integer").into()),
    },
    None => Ok(0),
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_parse_args_basic() {
    // HINCRBY key field 5
    let items = vec![
      Value::SimpleString("HINCRBY".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"field1".to_vec())),
      Value::BulkString(Some(b"5".to_vec())),
    ];

    let args = HIncrByCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mykey");
    assert_eq!(args.field, b"field1");
    assert_eq!(args.increment, 5);
  }

  #[test]
  fn test_parse_args_negative() {
    // HINCRBY key field -10
    let items = vec![
      Value::SimpleString("HINCRBY".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"field1".to_vec())),
      Value::BulkString(Some(b"-10".to_vec())),
    ];

    let args = HIncrByCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mykey");
    assert_eq!(args.field, b"field1");
    assert_eq!(args.increment, -10);
  }

  #[test]
  fn test_parse_args_integer_type() {
    // HINCRBY with Integer type
    let items = vec![
      Value::SimpleString("HINCRBY".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"field1".to_vec())),
      Value::Integer(42),
    ];

    let args = HIncrByCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mykey");
    assert_eq!(args.field, b"field1");
    assert_eq!(args.increment, 42);
  }

  #[test]
  fn test_parse_args_insufficient_args() {
    // HINCRBY key field (missing increment)
    let items = vec![
      Value::SimpleString("HINCRBY".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"field1".to_vec())),
    ];

    let result = HIncrByCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_too_many_args() {
    // HINCRBY key field 5 extra
    let items = vec![
      Value::SimpleString("HINCRBY".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"field1".to_vec())),
      Value::BulkString(Some(b"5".to_vec())),
      Value::BulkString(Some(b"extra".to_vec())),
    ];

    let result = HIncrByCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_invalid_increment() {
    // HINCRBY key field not_a_number
    let items = vec![
      Value::SimpleString("HINCRBY".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"field1".to_vec())),
      Value::BulkString(Some(b"not_a_number".to_vec())),
    ];

    let result = HIncrByCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_large_number() {
    // HINCRBY key field 9223372036854775807 (max i64)
    let items = vec![
      Value::SimpleString("HINCRBY".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"field1".to_vec())),
      Value::BulkString(Some(b"9223372036854775807".to_vec())),
    ];

    let args = HIncrByCommand::parse_args(&items).unwrap();
    assert_eq!(args.increment, i64::MAX);
  }

  #[test]
  fn test_parse_args_min_number() {
    // HINCRBY key field -9223372036854775808 (min i64)
    let items = vec![
      Value::SimpleString("HINCRBY".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"field1".to_vec())),
      Value::BulkString(Some(b"-9223372036854775808".to_vec())),
    ];

    let args = HIncrByCommand::parse_args(&items).unwrap();
    assert_eq!(args.increment, i64::MIN);
  }

  #[test]
  fn test_parse_args_overflow_number() {
    // HINCRBY key field 9223372036854775808 (overflow)
    let items = vec![
      Value::SimpleString("HINCRBY".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"field1".to_vec())),
      Value::BulkString(Some(b"9223372036854775808".to_vec())),
    ];

    let result = HIncrByCommand::parse_args(&items);
    assert!(result.is_err());
  }
}
