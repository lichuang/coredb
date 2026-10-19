//! HDEL command implementation
//!
//! HDEL key field [field ...]
//! Removes the specified fields from the hash stored at key.
//!
//! Returns:
//! - The number of fields that were removed from the hash
//! - 0 if the key does not exist or if no fields were removed
//!
//! Concurrency: field removal decrements the metadata's `size`, so two
//! concurrent HDELs reading the same metadata would each write
//! `size = read_size - removed` back — the last writer wins and the count
//! loses the other's removals (docs/bug.md §1.5, the mirror of the HSET
//! race). The read-modify-write is therefore guarded by a conditional
//! transaction that pins the observed metadata bytes; losers re-read and
//! retry with capped, jittered backoff. Absent, expired, or corrupt hashes
//! have nothing to remove and no state to race over, so they reply 0
//! without entering a transaction.

use rockraft::raft::types::{TxnCondition, TxnReply, TxnReq, UpsertKV};

use crate::encoding::{HashFieldValue, HashMetadata, TYPE_HASH};
use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::{MAX_CAS_RETRIES, backoff_delay, now_ms};
use async_trait::async_trait;

/// The current hash state observed for the target key.
enum CurrentHash {
  /// Key absent, expired, or corrupt: nothing to remove (0 reply).
  Empty,
  /// Present, unexpired, and a hash; carries its serialized metadata bytes.
  Hash {
    raw: Vec<u8>,
    metadata: HashMetadata,
  },
  /// Present but not a hash value: report WRONGTYPE instead of reading it.
  Other,
}

async fn observe_current(server: &Server, key: &str) -> Result<CurrentHash, CoreDbError> {
  let raw = match server.get(key).await? {
    Some(bytes) => bytes,
    None => return Ok(CurrentHash::Empty),
  };

  match HashMetadata::deserialize(&raw) {
    Ok(meta) if meta.get_type() != TYPE_HASH => Ok(CurrentHash::Other),
    Ok(meta) if meta.is_expired(now_ms()) => Ok(CurrentHash::Empty),
    Ok(meta) => Ok(CurrentHash::Hash {
      raw,
      metadata: meta,
    }),
    Err(_) => Ok(CurrentHash::Empty),
  }
}

/// HDEL command handler
pub struct HDelCommand;

#[async_trait]
impl Command for HDelCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    // Parse HDEL key field [field ...]
    // Minimum: HDEL key field (3 items)
    if items.len() < 3 {
      return Err(ProtocolError::WrongArgCount("hdel").into());
    }

    // Parse key
    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::InvalidArgument("key").into()),
    };

    // Parse fields to delete
    let mut fields = Vec::with_capacity(items.len() - 2);
    for item in items.iter().skip(2) {
      let field = match item {
        Value::BulkString(Some(data)) => data.clone(),
        Value::SimpleString(s) => s.as_bytes().to_vec(),
        _ => return Err(ProtocolError::InvalidArgument("field").into()),
      };
      fields.push(field);
    }

    for attempt in 0..MAX_CAS_RETRIES {
      let (raw_meta, mut metadata) = match observe_current(server, &key).await? {
        CurrentHash::Other => return Err(ProtocolError::WrongType.into()),
        // Absent/expired/corrupt hashes lose nothing and have no state to
        // race over; reply 0 without entering a transaction.
        CurrentHash::Empty => return Ok(Value::Integer(0)),
        CurrentHash::Hash { raw, metadata } => (raw, metadata),
      };

      let version = metadata.version;
      let mut deleted_count = 0i64;

      // Check each field and prepare delete entries, against the pinned
      // version space.
      let mut entries: Vec<UpsertKV> = Vec::new();

      for field in &fields {
        let sub_key_str = HashFieldValue::build_sub_key_hex(key.as_bytes(), version, field);

        // Check if field exists before deleting
        if let Ok(Some(_)) = server.get(&sub_key_str).await {
          // Field exists, prepare delete entry
          entries.push(UpsertKV::delete(sub_key_str));
          deleted_count += 1;
          metadata.decr_size();
        }
      }

      // If no fields to delete, return early: no writes happened, so the
      // metadata is untouched and a fresh observation would agree.
      if deleted_count == 0 {
        return Ok(Value::Integer(0));
      }

      // Add metadata update entry
      entries.push(UpsertKV::insert(key.clone(), &metadata.serialize()));

      // The condition pins the observed metadata bytes, so the `size`
      // decrement only lands if nothing else touched the hash at apply time.
      let reply = server
        .txn(TxnReq::new(vec![TxnCondition::eq(&key, &raw_meta)]).if_then_ops(entries))
        .await?;

      match reply {
        TxnReply::Success { branch: true, .. } => {
          return Ok(Value::Integer(deleted_count));
        }
        // Lost the race; back off (jittered, capped) so concurrent losers do
        // not re-compete in lockstep, then re-observe.
        _ => {
          tokio::time::sleep(backoff_delay(attempt)).await;
          continue;
        }
      }
    }

    Err(ProtocolError::Custom("ERR hdel retry limit exceeded").into())
  }
}

#[cfg(test)]
mod tests {
  #[test]
  fn test_hdel_params_parsing() {
    // Test would require a running server, so we just verify structure
    // Full integration tests should be in tests/ directory
  }
}
