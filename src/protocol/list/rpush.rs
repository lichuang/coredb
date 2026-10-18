//! RPUSH command implementation
//!
//! RPUSH key element [element ...]
//! Insert all the specified values at the tail of the list stored at key.
//! If key does not exist, it is created as empty list before performing the push.
//! When key holds a value that is not a list, an error is returned.
//!
//! Returns:
//! - The length of the list after the push operations (integer reply)
//!
//! Note: Elements are inserted one after the other from leftmost to rightmost.
//! `RPUSH mylist a b c` results in `[a, b, c]`.
//!
//! Concurrency: the element sub-key index is derived from the metadata's
//! `tail` cursor, so two concurrent pushes reading the same metadata would
//! compute the same sub-key and overwrite each other's elements
//! (docs/bug.md §1.3). The whole read-modify-write is therefore guarded by a
//! conditional transaction that pins the observed metadata bytes; losers
//! re-read and retry with capped, jittered backoff.

use rockraft::raft::types::{TxnCondition, TxnReply, TxnReq, UpsertKV};

use crate::encoding::{ListElementValue, ListMetadata, TYPE_LIST};
use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::{MAX_CAS_RETRIES, backoff_delay, now_ms};
use async_trait::async_trait;

pub struct RPushCommand;

impl RPushCommand {
  fn parse_args(items: &[Value]) -> Result<RPushArgs, ProtocolError> {
    if items.len() < 3 {
      return Err(ProtocolError::WrongArgCount("rpush"));
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::InvalidArgument("key")),
    };

    let mut elements = Vec::with_capacity(items.len() - 2);
    for item in &items[2..] {
      let elem = match item {
        Value::BulkString(Some(data)) => data.clone(),
        Value::SimpleString(s) => s.as_bytes().to_vec(),
        _ => return Err(ProtocolError::InvalidArgument("element")),
      };
      elements.push(elem);
    }

    Ok(RPushArgs { key, elements })
  }
}

struct RPushArgs {
  key: String,
  elements: Vec<Vec<u8>>,
}

/// The current list state observed for the target key.
enum CurrentList {
  /// Key absent: push starts a fresh list, guarded by `not_exists`.
  Absent,
  /// Present, unexpired, and a list; carries its serialized metadata bytes.
  List {
    raw: Vec<u8>,
    metadata: ListMetadata,
  },
  /// Present but expired: logically absent, yet its bytes still sit in the
  /// store, so the CAS must pin those bytes with `eq` — `not_exists` would
  /// never hold and the command would spin out its retry budget. The fresh
  /// list gets a new version, so old sub-keys become invisible.
  Expired { raw: Vec<u8> },
  /// Present but not a list value: report WRONGTYPE instead of overwriting.
  Other,
}

async fn observe_current(server: &Server, key: &str) -> Result<CurrentList, CoreDbError> {
  let raw = match server.get(key).await? {
    Some(bytes) => bytes,
    None => return Ok(CurrentList::Absent),
  };

  match ListMetadata::deserialize(&raw) {
    Ok(meta) if meta.get_type() != TYPE_LIST => Ok(CurrentList::Other),
    Ok(meta) if meta.is_expired(now_ms()) => Ok(CurrentList::Expired { raw }),
    Ok(meta) => Ok(CurrentList::List {
      raw,
      metadata: meta,
    }),
    Err(_) => Ok(CurrentList::Other),
  }
}

#[async_trait]
impl Command for RPushCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    let args = Self::parse_args(items)?;

    for attempt in 0..MAX_CAS_RETRIES {
      // Pin the observed metadata bytes: the sub-key indexes are derived from
      // `tail`, so the entries only land if the metadata is unchanged at
      // apply time. Overwrites are impossible because the winning writer's
      // tail was never used by any other successful push.
      let (condition, mut new_metadata) = match observe_current(server, &args.key).await? {
        CurrentList::Other => return Err(ProtocolError::WrongType.into()),
        CurrentList::Absent => {
          let metadata = ListMetadata::new();
          (TxnCondition::not_exists(&args.key), metadata)
        }
        // An expired list reads as absent, but its bytes are still stored:
        // pin them with `eq` (not_exists would never hold). The fresh list
        // carries a new version, so expired elements stay unreachable.
        CurrentList::Expired { raw } => {
          let metadata = ListMetadata::new();
          (TxnCondition::eq(&args.key, raw), metadata)
        }
        CurrentList::List { raw, metadata } => (TxnCondition::eq(&args.key, raw), metadata),
      };

      let mut entries: Vec<UpsertKV> = Vec::with_capacity(args.elements.len() + 1);

      // RPUSH mylist a b c → a goes to tail, b goes to tail+1, c goes to
      // tail+2. Final list order: [a, b, c].
      for (i, elem_data) in args.elements.iter().enumerate() {
        let index = new_metadata.tail + i as u64;
        let sub_key_str =
          ListElementValue::build_sub_key_hex(args.key.as_bytes(), new_metadata.version, index);
        let elem_value = ListElementValue::new(elem_data.clone());
        entries.push(UpsertKV::insert(sub_key_str, &elem_value.serialize()));
      }

      new_metadata.tail += args.elements.len() as u64;
      new_metadata.size += args.elements.len() as u64;
      entries.push(UpsertKV::insert(
        args.key.clone(),
        &new_metadata.serialize(),
      ));

      let reply = server
        .txn(TxnReq::new(vec![condition]).if_then_ops(entries))
        .await?;

      match reply {
        TxnReply::Success { branch: true, .. } => {
          return Ok(Value::Integer(new_metadata.size as i64));
        }
        // Lost the race; back off (jittered, capped) so concurrent losers do
        // not re-compete in lockstep, then re-observe.
        _ => {
          tokio::time::sleep(backoff_delay(attempt)).await;
          continue;
        }
      }
    }

    Err(ProtocolError::Custom("ERR rpush retry limit exceeded").into())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn bulk(data: &[u8]) -> Value {
    Value::BulkString(Some(data.to_vec()))
  }

  #[test]
  fn test_parse_args_basic() {
    let items = vec![
      Value::SimpleString("RPUSH".to_string()),
      bulk(b"mylist"),
      bulk(b"hello"),
    ];

    let args = RPushCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mylist");
    assert_eq!(args.elements.len(), 1);
    assert_eq!(args.elements[0], b"hello");
  }

  #[test]
  fn test_parse_args_multiple_elements() {
    let items = vec![
      Value::SimpleString("RPUSH".to_string()),
      bulk(b"mylist"),
      bulk(b"a"),
      bulk(b"b"),
      bulk(b"c"),
    ];

    let args = RPushCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mylist");
    assert_eq!(args.elements.len(), 3);
    assert_eq!(args.elements[0], b"a");
    assert_eq!(args.elements[1], b"b");
    assert_eq!(args.elements[2], b"c");
  }

  #[test]
  fn test_parse_args_insufficient() {
    let items = vec![Value::SimpleString("RPUSH".to_string()), bulk(b"mylist")];
    let result = RPushCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_no_args() {
    let items = vec![Value::SimpleString("RPUSH".to_string())];
    let result = RPushCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_simple_string_key() {
    let items = vec![
      Value::SimpleString("RPUSH".to_string()),
      Value::SimpleString("mylist".to_string()),
      Value::SimpleString("value".to_string()),
    ];

    let args = RPushCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mylist");
    assert_eq!(args.elements.len(), 1);
    assert_eq!(args.elements[0], b"value");
  }

  #[test]
  fn test_parse_args_binary_element() {
    let items = vec![
      Value::SimpleString("RPUSH".to_string()),
      bulk(b"mylist"),
      bulk(b"\x00\x01\xff"),
    ];

    let args = RPushCommand::parse_args(&items).unwrap();
    assert_eq!(args.elements[0], b"\x00\x01\xff");
  }

  #[test]
  fn test_parse_args_empty_element() {
    let items = vec![
      Value::SimpleString("RPUSH".to_string()),
      bulk(b"mylist"),
      bulk(b""),
    ];

    let args = RPushCommand::parse_args(&items).unwrap();
    assert_eq!(args.elements.len(), 1);
    assert!(args.elements[0].is_empty());
  }

  #[test]
  fn test_parse_args_invalid_key_type() {
    let items = vec![
      Value::SimpleString("RPUSH".to_string()),
      Value::Integer(42),
      bulk(b"element"),
    ];
    let result = RPushCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_invalid_element_type() {
    let items = vec![
      Value::SimpleString("RPUSH".to_string()),
      bulk(b"mylist"),
      Value::Integer(42),
    ];
    let result = RPushCommand::parse_args(&items);
    assert!(result.is_err());
  }
}
