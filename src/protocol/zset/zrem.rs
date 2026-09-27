//! ZREM command implementation
//!
//! ZREM key member [member ...]
//! Removes the specified members from the sorted set stored at key.
//! Returns the number of members actually removed.
//!
//! Concurrency: member removal decrements the metadata's `size`, so two
//! concurrent ZREMs reading the same metadata would each write
//! `size = read_size - removed` back — the last writer wins and the count
//!(docs/bug.md §1.4, same pattern as ZADD). The read-modify-write is
//! therefore guarded by a conditional transaction that pins the observed
//! metadata bytes; losers re-read and retry with capped, jittered backoff.

use rockraft::raft::types::{TxnCondition, TxnReply, TxnReq, UpsertKV};

use crate::encoding::{TYPE_ZSET, ZSetMemberValue, ZSetMetadata};
use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::{MAX_CAS_RETRIES, backoff_delay, now_ms};
use async_trait::async_trait;

struct ZRemArgs {
  key: String,
  members: Vec<Vec<u8>>,
}

pub struct ZRemCommand;

impl ZRemCommand {
  fn parse_args(items: &[Value]) -> Result<ZRemArgs, ProtocolError> {
    if items.len() < 3 {
      return Err(ProtocolError::WrongArgCount("zrem"));
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::InvalidArgument("key")),
    };

    let mut members = Vec::with_capacity(items.len() - 2);
    for item in &items[2..] {
      let member = match item {
        Value::BulkString(Some(data)) => data.clone(),
        Value::SimpleString(s) => s.as_bytes().to_vec(),
        _ => return Err(ProtocolError::InvalidArgument("member")),
      };
      members.push(member);
    }

    Ok(ZRemArgs { key, members })
  }
}

/// The current zset state observed for the target key.
enum CurrentZSet {
  /// Key absent, expired, or corrupt: nothing to remove (0 reply).
  Empty,
  /// Present, unexpired, and a zset; carries its serialized metadata bytes.
  ZSet {
    raw: Vec<u8>,
    metadata: ZSetMetadata,
  },
  /// Present but not a zset value: report WRONGTYPE instead of reading it.
  Other,
}

async fn observe_current(server: &Server, key: &str) -> Result<CurrentZSet, CoreDbError> {
  let raw = match server.get(key).await? {
    Some(bytes) => bytes,
    None => return Ok(CurrentZSet::Empty),
  };

  match ZSetMetadata::deserialize(&raw) {
    Ok(meta) if meta.get_type() != TYPE_ZSET => Ok(CurrentZSet::Other),
    Ok(meta) if meta.is_expired(now_ms()) || meta.size == 0 => Ok(CurrentZSet::Empty),
    Ok(meta) => Ok(CurrentZSet::ZSet {
      raw,
      metadata: meta,
    }),
    Err(_) => Ok(CurrentZSet::Empty),
  }
}

#[async_trait]
impl Command for ZRemCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    let args = Self::parse_args(items)?;

    for attempt in 0..MAX_CAS_RETRIES {
      let (raw_meta, mut metadata) = match observe_current(server, &args.key).await? {
        CurrentZSet::Other => return Err(ProtocolError::WrongType.into()),
        // No removable state: expired/absent/corrupt/empty zsets lose
        // nothing, so reply 0 without touching the key (no race window).
        CurrentZSet::Empty => return Ok(Value::Integer(0)),
        CurrentZSet::ZSet { raw, metadata } => (raw, metadata),
      };

      let version = metadata.version;
      let mut removed_count = 0i64;
      let mut entries: Vec<UpsertKV> = Vec::new();

      for member in &args.members {
        let sub_key_str = ZSetMemberValue::build_sub_key_hex(args.key.as_bytes(), version, member);

        if let Ok(Some(_)) = server.get(&sub_key_str).await {
          entries.push(UpsertKV::delete(sub_key_str));
          removed_count += 1;
          metadata.decr_size();
        }
      }

      if removed_count == 0 {
        // Nothing to remove: no writes happened, so reply 0 directly. The
        // metadata is untouched and another writer may have changed it in
        // the meantime; a fresh observation would give the same answer.
        return Ok(Value::Integer(0));
      }

      entries.push(UpsertKV::insert(args.key.clone(), &metadata.serialize()));

      // The condition pins the observed metadata bytes so the `size`
      // decrement only lands if nothing else touched the zset at apply time.
      let reply = server
        .txn(TxnReq::new(vec![TxnCondition::eq(&args.key, &raw_meta)]).if_then_ops(entries))
        .await?;

      match reply {
        TxnReply::Success { branch: true, .. } => {
          return Ok(Value::Integer(removed_count));
        }
        // Lost the race; back off (jittered, capped) so concurrent losers do
        // not re-compete in lockstep, then re-observe.
        _ => {
          tokio::time::sleep(backoff_delay(attempt)).await;
          continue;
        }
      }
    }

    Err(ProtocolError::Custom("ERR zrem retry limit exceeded").into())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn bulk(data: &[u8]) -> Value {
    Value::BulkString(Some(data.to_vec()))
  }

  fn ss(s: &str) -> Value {
    Value::SimpleString(s.to_string())
  }

  #[test]
  fn test_parse_args_basic() {
    let items = vec![ss("ZREM"), bulk(b"myzset"), bulk(b"member1")];

    let args = ZRemCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "myzset");
    assert_eq!(args.members.len(), 1);
    assert_eq!(args.members[0], b"member1");
  }

  #[test]
  fn test_parse_args_multiple_members() {
    let items = vec![
      ss("ZREM"),
      bulk(b"myzset"),
      bulk(b"a"),
      bulk(b"b"),
      bulk(b"c"),
    ];

    let args = ZRemCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "myzset");
    assert_eq!(args.members.len(), 3);
    assert_eq!(args.members[0], b"a");
    assert_eq!(args.members[1], b"b");
    assert_eq!(args.members[2], b"c");
  }

  #[test]
  fn test_parse_args_insufficient() {
    let items = vec![ss("ZREM"), bulk(b"myzset")];
    let result = ZRemCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_no_args() {
    let items = vec![ss("ZREM")];
    let result = ZRemCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_simple_string_key() {
    let items = vec![
      ss("ZREM"),
      Value::SimpleString("myzset".to_string()),
      Value::SimpleString("member".to_string()),
    ];

    let args = ZRemCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "myzset");
    assert_eq!(args.members.len(), 1);
    assert_eq!(args.members[0], b"member");
  }

  #[test]
  fn test_parse_args_binary_member() {
    let items = vec![ss("ZREM"), bulk(b"myzset"), bulk(b"\x00\x01\xff")];

    let args = ZRemCommand::parse_args(&items).unwrap();
    assert_eq!(args.members[0], b"\x00\x01\xff");
  }

  #[test]
  fn test_parse_args_empty_member() {
    let items = vec![ss("ZREM"), bulk(b"myzset"), bulk(b"")];

    let args = ZRemCommand::parse_args(&items).unwrap();
    assert_eq!(args.members.len(), 1);
    assert!(args.members[0].is_empty());
  }

  #[test]
  fn test_parse_args_invalid_key_type() {
    let items = vec![ss("ZREM"), Value::Integer(42), bulk(b"member")];

    let result = ZRemCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_invalid_member_type() {
    let items = vec![ss("ZREM"), bulk(b"myzset"), Value::Integer(42)];

    let result = ZRemCommand::parse_args(&items);
    assert!(result.is_err());
  }
}
