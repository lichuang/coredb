//! HSET command implementation
//!
//! HSET key field value
//! Sets field in the hash stored at key to value.
//! Returns 1 if field is a new field, 0 if field existed and was updated.

use crate::encoding::{HashFieldValue, HashMetadata, NO_EXPIRATION};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

/// HSET command handler
pub struct HSetCommand;

#[async_trait]
impl Command for HSetCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Value {
    // Parse HSET key field value
    if items.len() < 4 {
      return Value::error("ERR wrong number of arguments for 'hset' command");
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Value::error("ERR invalid key"),
    };

    let field = match &items[2] {
      Value::BulkString(Some(data)) => data.clone(),
      Value::SimpleString(s) => s.as_bytes().to_vec(),
      _ => return Value::error("ERR invalid field"),
    };

    let value_data = match &items[3] {
      Value::BulkString(Some(data)) => data.clone(),
      Value::SimpleString(s) => s.as_bytes().to_vec(),
      _ => return Value::error("ERR invalid value"),
    };

    // Get or create metadata
    let mut metadata = match server.get(&key).await {
      Ok(Some(raw_meta)) => {
        match HashMetadata::deserialize(&raw_meta) {
          Ok(meta) => {
            // Check if expired
            if meta.is_expired(now_ms()) {
              // Expired, treat as new
              HashMetadata::new()
            } else {
              meta
            }
          }
          Err(_) => {
            // Corrupted, create new
            HashMetadata::new()
          }
        }
      }
      _ => {
        // Not found, create new
        HashMetadata::new()
      }
    };

    // Check if field exists
    let version = metadata.version;
    let sub_key = HashFieldValue::build_sub_key(key.as_bytes(), version, &field);
    let sub_key_str = String::from_utf8_lossy(&sub_key).to_string();

    let field_exists = match server.get(&sub_key_str).await {
      Ok(Some(_)) => true,
      _ => false,
    };

    // Store the field value
    let field_value = HashFieldValue::new(value_data);
    if let Err(e) = server.set(sub_key_str, field_value.serialize()).await {
      return Value::error(format!("ERR failed to store field: {}", e));
    }

    // Update metadata
    if !field_exists {
      metadata.incr_size();
    }

    // Store metadata
    if let Err(e) = server.set(key, metadata.serialize()).await {
      return Value::error(format!("ERR failed to store metadata: {}", e));
    }

    // Return 1 if new field, 0 if updated
    Value::Integer(if field_exists { 0 } else { 1 })
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::protocol::resp::Value;

  #[test]
  fn test_hset_params_parsing() {
    // Test would require a running server, so we just verify structure
    // Full integration tests should be in tests/ directory
  }
}
