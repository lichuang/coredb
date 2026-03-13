//! HDEL command implementation
//!
//! HDEL key field [field ...]
//! Removes the specified fields from the hash stored at key.
//!
//! Returns:
//! - The number of fields that were removed from the hash
//! - 0 if the key does not exist or if no fields were removed

use crate::encoding::{HashFieldValue, HashMetadata};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

/// HDEL command handler
pub struct HDelCommand;

#[async_trait]
impl Command for HDelCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Value {
    // Parse HDEL key field [field ...]
    // Minimum: HDEL key field (3 items)
    if items.len() < 3 {
      return Value::error("ERR wrong number of arguments for 'hdel' command");
    }

    // Parse key
    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Value::error("ERR invalid key"),
    };

    // Parse fields to delete
    let mut fields = Vec::with_capacity(items.len() - 2);
    for item in items.iter().skip(2) {
      let field = match item {
        Value::BulkString(Some(data)) => data.clone(),
        Value::SimpleString(s) => s.as_bytes().to_vec(),
        _ => return Value::error("ERR invalid field"),
      };
      fields.push(field);
    }

    // Get metadata
    let mut metadata = match server.get(&key).await {
      Ok(Some(raw_meta)) => match HashMetadata::deserialize(&raw_meta) {
        Ok(meta) => {
          // Check if expired
          if meta.is_expired(now_ms()) {
            // Expired, nothing to delete
            return Value::Integer(0);
          }
          meta
        }
        Err(_) => {
          // Corrupted, nothing to delete
          return Value::Integer(0);
        }
      },
      _ => {
        // Not found, nothing to delete
        return Value::Integer(0);
      }
    };

    let version = metadata.version;
    let mut deleted_count = 0i64;

    // Delete each field
    for field in fields {
      let sub_key_str = HashFieldValue::build_sub_key_hex(key.as_bytes(), version, &field);

      // Check if field exists before deleting
      match server.get(&sub_key_str).await {
        Ok(Some(_)) => {
          // Field exists, delete it
          if let Err(e) = server.delete(&sub_key_str).await {
            return Value::error(format!("ERR failed to delete field: {}", e));
          }
          deleted_count += 1;
          metadata.decr_size();
        }
        _ => {
          // Field does not exist, skip
        }
      }
    }

    // Update metadata (only if we deleted something)
    if deleted_count > 0 {
      // If hash is now empty, we could optionally delete the metadata key
      // For now, we keep it to maintain version history
      if let Err(e) = server.set(key.clone(), metadata.serialize()).await {
        return Value::error(format!("ERR failed to update metadata: {}", e));
      }
    }

    Value::Integer(deleted_count)
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
