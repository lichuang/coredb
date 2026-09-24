//! LPOP command implementation
//!
//! LPOP key [count]
//! Removes and returns the first elements of the list stored at key.
//! Without `count` returns a single bulk string (nil when empty);
//! with `count` returns an array.
//!
//! Concurrency: the popped sub-key indexes are derived from the metadata's
//! `head` cursor, so two concurrent pops reading the same metadata would
//! return (and delete) the same element — one element delivered twice while
//! another is never returned (docs/bug.md §1.3). The read-modify-write is
//! therefore guarded by a conditional transaction that pins the observed
//! metadata bytes; losers re-read and retry with capped, jittered backoff.

use rockraft::raft::types::{TxnCondition, TxnReply, TxnReq, UpsertKV};

use crate::encoding::{ListElementValue, ListMetadata, TYPE_LIST};
use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::{MAX_CAS_RETRIES, backoff_delay, now_ms};
use async_trait::async_trait;

pub struct LPopCommand;

struct PopArgs {
  key: String,
  count: Option<u64>,
}

impl LPopCommand {
  fn parse_args(items: &[Value]) -> Result<PopArgs, ProtocolError> {
    if items.len() < 2 {
      return Err(ProtocolError::WrongArgCount("lpop"));
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::InvalidArgument("key")),
    };

    let count = if items.len() >= 3 {
      match &items[2] {
        Value::BulkString(Some(data)) => {
          let s = String::from_utf8_lossy(data);
          match s.parse::<u64>() {
            Ok(n) => Some(n),
            Err(_) => return Err(ProtocolError::NotAnInteger),
          }
        }
        Value::SimpleString(s) => match s.parse::<u64>() {
          Ok(n) => Some(n),
          Err(_) => return Err(ProtocolError::NotAnInteger),
        },
        Value::Integer(n) => {
          if *n < 0 {
            return Err(ProtocolError::NotAnInteger);
          }
          Some(*n as u64)
        }
        _ => {
          return Err(ProtocolError::NotAnInteger);
        }
      }
    } else {
      None
    };

    Ok(PopArgs { key, count })
  }
}

/// The current list state observed for the target key.
enum CurrentList {
  /// Key absent, expired, corrupt, or empty: nothing to pop (nil reply).
  Empty,
  /// Present, unexpired, and a non-empty list; carries its serialized
  /// metadata bytes.
  List {
    raw: Vec<u8>,
    metadata: ListMetadata,
  },
  /// Present but not a list value: report WRONGTYPE instead of reading it.
  Other,
}

async fn observe_current(server: &Server, key: &str) -> Result<CurrentList, CoreDbError> {
  let raw = match server.get(key).await? {
    Some(bytes) => bytes,
    None => return Ok(CurrentList::Empty),
  };

  match ListMetadata::deserialize(&raw) {
    Ok(meta) if meta.get_type() != TYPE_LIST => Ok(CurrentList::Other),
    Ok(meta) if meta.is_expired(now_ms()) || meta.size == 0 => Ok(CurrentList::Empty),
    Ok(meta) => Ok(CurrentList::List {
      raw,
      metadata: meta,
    }),
    Err(_) => Ok(CurrentList::Empty),
  }
}

#[async_trait]
impl Command for LPopCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    let args = Self::parse_args(items)?;

    for attempt in 0..MAX_CAS_RETRIES {
      let (raw_meta, metadata) = match observe_current(server, &args.key).await? {
        CurrentList::Other => return Err(ProtocolError::WrongType.into()),
        CurrentList::Empty => return Ok(Value::BulkString(None)),
        CurrentList::List { raw, metadata } => (raw, metadata),
      };

      let pop_count = args.count.unwrap_or(1).min(metadata.size);
      let version = metadata.version;
      let head = metadata.head;

      // Read the payloads of the elements to pop. The condition pins the
      // metadata, so these sub-keys are ours to delete iff the metadata is
      // unchanged at apply time.
      let mut results: Vec<Value> = Vec::with_capacity(pop_count as usize);
      for i in 0..pop_count {
        let sub_key_str =
          ListElementValue::build_sub_key_hex(args.key.as_bytes(), version, head + i);
        match server.get(&sub_key_str).await? {
          Some(raw_elem) => match ListElementValue::deserialize(&raw_elem) {
            Ok(elem) => results.push(Value::BulkString(Some(elem.data))),
            Err(_) => break,
          },
          // The sub-key is gone while the metadata still claims it: the list
          // changed under us; re-observe instead of popping nothing.
          None => break,
        }
      }

      if results.is_empty() {
        tokio::time::sleep(backoff_delay(attempt)).await;
        continue;
      }

      let actual_popped = results.len() as u64;
      let new_size = metadata.size - actual_popped;

      let mut entries: Vec<UpsertKV> = Vec::with_capacity(actual_popped as usize + 1);
      for i in 0..actual_popped {
        let sub_key_str =
          ListElementValue::build_sub_key_hex(args.key.as_bytes(), version, head + i);
        entries.push(UpsertKV::delete(sub_key_str));
      }

      let mut new_meta = metadata.clone();
      new_meta.head = head + actual_popped;
      new_meta.size = new_size;
      if new_size == 0 {
        // List drained: drop the metadata key entirely.
        entries.push(UpsertKV::delete(args.key.clone()));
      } else {
        entries.push(UpsertKV::insert(args.key.clone(), &new_meta.serialize()));
      }

      // Whether we overwrite the metadata or delete it, the transaction only
      // applies if the observed metadata bytes are unchanged.
      let condition = TxnCondition::eq(&args.key, &raw_meta);
      let reply = server
        .txn(TxnReq::new(vec![condition]).if_then_ops(entries))
        .await?;

      match reply {
        TxnReply::Success { branch: true, .. } => {
          return Ok(match args.count {
            None => results
              .into_iter()
              .next()
              .unwrap_or(Value::BulkString(None)),
            Some(_) => Value::Array(Some(results)),
          });
        }
        // Lost the race; back off (jittered, capped) so concurrent losers do
        // not re-compete in lockstep, then re-observe.
        _ => {
          tokio::time::sleep(backoff_delay(attempt)).await;
          continue;
        }
      }
    }

    Err(ProtocolError::Custom("ERR lpop retry limit exceeded").into())
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
    let items = vec![Value::SimpleString("LPOP".to_string()), bulk(b"mylist")];
    let args = LPopCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mylist");
    assert_eq!(args.count, None);
  }

  #[test]
  fn test_parse_args_with_count() {
    let items = vec![
      Value::SimpleString("LPOP".to_string()),
      bulk(b"mylist"),
      Value::SimpleString("3".to_string()),
    ];
    let args = LPopCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mylist");
    assert_eq!(args.count, Some(3));
  }

  #[test]
  fn test_parse_args_with_count_integer() {
    let items = vec![
      Value::SimpleString("LPOP".to_string()),
      bulk(b"mylist"),
      Value::Integer(5),
    ];
    let args = LPopCommand::parse_args(&items).unwrap();
    assert_eq!(args.count, Some(5));
  }

  #[test]
  fn test_parse_args_no_key() {
    let items = vec![Value::SimpleString("LPOP".to_string())];
    let result = LPopCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_invalid_count() {
    let items = vec![
      Value::SimpleString("LPOP".to_string()),
      bulk(b"mylist"),
      bulk(b"abc"),
    ];
    let result = LPopCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_negative_count() {
    let items = vec![
      Value::SimpleString("LPOP".to_string()),
      bulk(b"mylist"),
      Value::Integer(-1),
    ];
    let result = LPopCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_invalid_key_type() {
    let items = vec![Value::SimpleString("LPOP".to_string()), Value::Integer(42)];
    let result = LPopCommand::parse_args(&items);
    assert!(result.is_err());
  }
}
