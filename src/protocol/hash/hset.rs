//! HSET command implementation
//!
//! HSET key field value [field value ...]
//! Sets the specified fields to their respective values in the hash stored at key.
//!
//! Returns:
//! - The number of fields that were added (not updated)
//!
//! Concurrency: field sub-keys are qualified by the metadata's `version`, and
//! the field count is tracked by `size`. Concurrent HSETs that read the same
//! metadata would each write `size = read_size + added` back — the last
//! writer wins and the count loses every other writer's additions
//! (docs/bug.md §1.5), while writers whose `version` regeneration lands on a
//! different millisecond file fields into sub-key spaces that the surviving
//! metadata makes invisible. The read-modify-write is therefore guarded by a
//! conditional transaction that pins the observed metadata bytes; losers
//! re-read and retry with capped, jittered backoff.
//!
//! Note: This command uses a conditional transaction so that all fields and
//! metadata are written together as a single atomic, conflict-checked unit.

use rockraft::raft::types::{TxnCondition, TxnReply, TxnReq, UpsertKV};

use crate::encoding::{HashFieldValue, HashMetadata};
use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::{MAX_CAS_RETRIES, backoff_delay, now_ms};
use async_trait::async_trait;

/// Parsed HSET arguments
#[derive(Debug)]
struct HSetArgs {
  key: String,
  fields: Vec<(Vec<u8>, Vec<u8>)>, // (field, value) pairs
}

/// HSET command handler
pub struct HSetCommand;

impl HSetCommand {
  /// Parse arguments from RESP items
  /// Format: HSET key field value [field value ...]
  fn parse_args(items: &[Value]) -> Result<HSetArgs, ProtocolError> {
    // Minimum: HSET key field value (4 items)
    if items.len() < 4 {
      return Err(ProtocolError::WrongArgCount("hset"));
    }

    // Parse key
    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::InvalidArgument("key")),
    };

    // Parse field-value pairs from items[2..]
    let field_count = items.len() - 2; // items[2] onwards
    if !field_count.is_multiple_of(2) {
      return Err(ProtocolError::WrongArgCount("hset"));
    }

    let mut fields = Vec::with_capacity(field_count / 2);
    let mut i = 2;
    while i < items.len() {
      let field = match &items[i] {
        Value::BulkString(Some(data)) => data.clone(),
        Value::SimpleString(s) => s.as_bytes().to_vec(),
        _ => return Err(ProtocolError::InvalidArgument("field")),
      };

      let value = match &items[i + 1] {
        Value::BulkString(Some(data)) => data.clone(),
        Value::SimpleString(s) => s.as_bytes().to_vec(),
        _ => return Err(ProtocolError::InvalidArgument("value")),
      };

      fields.push((field, value));
      i += 2;
    }

    Ok(HSetArgs { key, fields })
  }
}

/// The current hash state observed for the target key.
enum CurrentHash {
  /// Key absent: HSET creates a fresh hash, guarded by `not_exists`.
  Absent,
  /// Present, unexpired, and a hash; carries its serialized metadata bytes.
  Hash {
    raw: Vec<u8>,
    metadata: HashMetadata,
  },
  /// Present but expired: logically absent, yet its bytes still sit in the
  /// store, so the CAS must pin those bytes with `eq` — `not_exists` would
  /// never hold and the command would spin out its retry budget. The fresh
  /// hash gets a new version, so old field sub-keys become invisible.
  Expired { raw: Vec<u8> },
  /// Present but not a hash value: report WRONGTYPE instead of overwriting.
  Other,
}

async fn observe_current(server: &Server, key: &str) -> Result<CurrentHash, CoreDbError> {
  let raw = match server.get(key).await? {
    Some(bytes) => bytes,
    None => return Ok(CurrentHash::Absent),
  };

  match HashMetadata::deserialize(&raw) {
    Ok(meta) if meta.get_type() != crate::encoding::TYPE_HASH => Ok(CurrentHash::Other),
    Ok(meta) if meta.is_expired(now_ms()) => Ok(CurrentHash::Expired { raw }),
    Ok(meta) => Ok(CurrentHash::Hash {
      raw,
      metadata: meta,
    }),
    Err(_) => Ok(CurrentHash::Other),
  }
}

#[async_trait]
impl Command for HSetCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    let args = Self::parse_args(items)?;

    for attempt in 0..MAX_CAS_RETRIES {
      // Pin the observed metadata bytes: field sub-keys live in the version
      // space the metadata carries, and `size` counts the fields, so the
      // writes only land if the metadata is unchanged at apply time.
      let (condition, mut metadata) = match observe_current(server, &args.key).await? {
        CurrentHash::Other => return Err(ProtocolError::WrongType.into()),
        CurrentHash::Absent => (TxnCondition::not_exists(&args.key), HashMetadata::new()),
        // An expired hash reads as absent, but its bytes are still stored:
        // pin them with `eq` (not_exists would never hold). The fresh hash
        // carries a new version, so expired fields stay unreachable.
        CurrentHash::Expired { raw } => (TxnCondition::eq(&args.key, raw), HashMetadata::new()),
        CurrentHash::Hash { raw, metadata } => (TxnCondition::eq(&args.key, raw), metadata),
      };

      let version = metadata.version;
      let mut added_count = 0i64;

      // Prepare field entries; existence checks run against the pinned
      // version space, so a field is only "new" relative to this metadata.
      let mut entries: Vec<UpsertKV> = Vec::new();

      for (field, value_data) in &args.fields {
        let sub_key_str = HashFieldValue::build_sub_key_hex(args.key.as_bytes(), version, field);

        let field_exists = (server.get(&sub_key_str).await?).is_some();
        if !field_exists {
          metadata.incr_size();
          added_count += 1;
        }

        let field_value = HashFieldValue::new(value_data.clone());
        entries.push(UpsertKV::insert(sub_key_str, &field_value.serialize()));
      }

      entries.push(UpsertKV::insert(args.key.clone(), &metadata.serialize()));

      let reply = server
        .txn(TxnReq::new(vec![condition]).if_then_ops(entries))
        .await?;

      match reply {
        TxnReply::Success { branch: true, .. } => {
          return Ok(Value::Integer(added_count));
        }
        // Lost the race; back off (jittered, capped) so concurrent losers do
        // not re-compete in lockstep, then re-observe.
        _ => {
          tokio::time::sleep(backoff_delay(attempt)).await;
          continue;
        }
      }
    }

    Err(ProtocolError::Custom("ERR hset retry limit exceeded").into())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_parse_args_basic() {
    // HSET key field value
    let items = vec![
      Value::SimpleString("HSET".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"field1".to_vec())),
      Value::BulkString(Some(b"value1".to_vec())),
    ];

    let args = HSetCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mykey");
    assert_eq!(args.fields.len(), 1);
    assert_eq!(args.fields[0].0, b"field1");
    assert_eq!(args.fields[0].1, b"value1");
  }

  #[test]
  fn test_parse_args_multiple_fields() {
    // HSET key f1 v1 f2 v2 f3 v3
    let items = vec![
      Value::SimpleString("HSET".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"f1".to_vec())),
      Value::BulkString(Some(b"v1".to_vec())),
      Value::BulkString(Some(b"f2".to_vec())),
      Value::BulkString(Some(b"v2".to_vec())),
      Value::BulkString(Some(b"f3".to_vec())),
      Value::BulkString(Some(b"v3".to_vec())),
    ];

    let args = HSetCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mykey");
    assert_eq!(args.fields.len(), 3);
    assert_eq!(args.fields[0].0, b"f1");
    assert_eq!(args.fields[0].1, b"v1");
    assert_eq!(args.fields[1].0, b"f2");
    assert_eq!(args.fields[1].1, b"v2");
    assert_eq!(args.fields[2].0, b"f3");
    assert_eq!(args.fields[2].1, b"v3");
  }

  #[test]
  fn test_parse_args_insufficient_args() {
    // HSET key (missing field and value)
    let items = vec![
      Value::SimpleString("HSET".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
    ];

    let result = HSetCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_odd_field_count() {
    // HSET key f1 v1 f2 (missing v2)
    let items = vec![
      Value::SimpleString("HSET".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"f1".to_vec())),
      Value::BulkString(Some(b"v1".to_vec())),
      Value::BulkString(Some(b"f2".to_vec())),
    ];

    let result = HSetCommand::parse_args(&items);
    assert!(result.is_err());
  }
}
