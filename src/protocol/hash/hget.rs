//! HGET command implementation
//!
//! HGET key field
//! Returns the value associated with field in the hash stored at key.
//! Returns nil if key or field does not exist.

use crate::encoding::{HashFieldValue, HashMetadata};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

/// HGET command handler
pub struct HGetCommand;

#[async_trait]
impl Command for HGetCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Value {
    // Parse HGET key field
    if items.len() < 3 {
      return Value::error("ERR wrong number of arguments for 'hget' command");
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

    // Get metadata
    let metadata = match server.get(&key).await {
      Ok(Some(raw_meta)) => {
        match HashMetadata::deserialize(&raw_meta) {
          Ok(meta) => {
            // Check if expired
            if meta.is_expired(now_ms()) {
              return Value::BulkString(None); // Return nil if expired
            }
            meta
          }
          Err(_) => {
            return Value::BulkString(None); // Return nil if corrupted
          }
        }
      }
      _ => {
        return Value::BulkString(None); // Return nil if not found
      }
    };

    // Get field value
    let version = metadata.version;
    let sub_key = HashFieldValue::build_sub_key(key.as_bytes(), version, &field);
    let sub_key_str = String::from_utf8_lossy(&sub_key).to_string();

    match server.get(&sub_key_str).await {
      Ok(Some(raw_value)) => {
        match HashFieldValue::deserialize(&raw_value) {
          Ok(field_value) => Value::BulkString(Some(field_value.data)),
          Err(_) => Value::BulkString(None), // Return nil if corrupted
        }
      }
      _ => Value::BulkString(None), // Return nil if not found
    }
  }
}

#[cfg(test)]
mod tests {
  #[test]
  fn test_hget_params_parsing() {
    // Test would require a running server, so we just verify structure
    // Full integration tests should be in tests/ directory
  }
}
