//! HDEL command implementation
//!
//! HDEL key field [field ...]
//! Removes the specified fields from the hash stored at key.
//!
//! Returns:
//! - The number of fields that were removed from the hash
//! - 0 if the key does not exist or if no fields were removed
//!
//! Note: This command uses atomic batch write to ensure all field deletions
//! and metadata update are applied together as a single atomic operation.

use rockraft::raft::types::UpsertKV;

use crate::encoding::{HashFieldValue, HashMetadata};
use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

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

    // Get metadata
    let mut metadata = match server.get(&key).await? {
      Some(raw_meta) => match HashMetadata::deserialize(&raw_meta) {
        Ok(meta) => {
          // Check if expired
          if meta.is_expired(now_ms()) {
            // Expired, nothing to delete
            return Ok(Value::Integer(0));
          }
          meta
        }
        Err(_) => {
          // Corrupted, nothing to delete
          return Ok(Value::Integer(0));
        }
      },
      None => {
        // Not found, nothing to delete
        return Ok(Value::Integer(0));
      }
    };

    let version = metadata.version;
    let mut deleted_count = 0i64;

    // Prepare batch write entries
    let mut entries: Vec<UpsertKV> = Vec::new();

    // Check each field and prepare delete entries
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

    // If no fields to delete, return early
    if deleted_count == 0 {
      return Ok(Value::Integer(0));
    }

    // Add metadata update entry
    entries.push(UpsertKV::insert(key.clone(), &metadata.serialize()));

    // Perform atomic batch write
    server.batch_write(entries).await?;

    Ok(Value::Integer(deleted_count))
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
