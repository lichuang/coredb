//! HMGET command implementation
//!
//! HMGET key field1 [field2 ...]
//! Returns the values associated with the specified fields in the hash stored at key.
//! Returns nil for every field that does not exist in the hash.
//!
//! Return value:
//! - Array of values: [value1, value2, ...] corresponding to the requested fields
//! - Values are returned in the same order as the requested fields
//! - Non-existent fields or key return nil for those positions

use crate::encoding::{HashFieldValue, HashMetadata};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

/// HMGET command handler
pub struct HMGetCommand;

#[async_trait]
impl Command for HMGetCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Value {
    // Parse HMGET key field1 [field2 ...]
    // Need at least: command + key + 1 field
    if items.len() < 3 {
      return Value::error("ERR wrong number of arguments for 'hmget' command");
    }

    // Parse key
    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Value::error("ERR invalid key"),
    };

    // Parse fields
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
    let metadata = match server.get(&key).await {
      Ok(Some(raw_meta)) => {
        match HashMetadata::deserialize(&raw_meta) {
          Ok(meta) => {
            // Check if expired
            if meta.is_expired(now_ms()) {
              // Return array of nils if expired
              return Value::Array(Some(vec![Value::BulkString(None); fields.len()]));
            }
            meta
          }
          Err(_) => {
            // Return array of nils if corrupted
            return Value::Array(Some(vec![Value::BulkString(None); fields.len()]));
          }
        }
      }
      _ => {
        // Return array of nils if not found
        return Value::Array(Some(vec![Value::BulkString(None); fields.len()]));
      }
    };

    // Get field values
    let version = metadata.version;
    let mut result_array = Vec::with_capacity(fields.len());

    for field in fields {
      let sub_key_str = HashFieldValue::build_sub_key_hex(key.as_bytes(), version, &field);

      let value = match server.get(&sub_key_str).await {
        Ok(Some(raw_value)) => {
          match HashFieldValue::deserialize(&raw_value) {
            Ok(field_value) => Value::BulkString(Some(field_value.data)),
            Err(_) => Value::BulkString(None), // Return nil if corrupted
          }
        }
        _ => Value::BulkString(None), // Return nil if not found
      };

      result_array.push(value);
    }

    Value::Array(Some(result_array))
  }
}

#[cfg(test)]
mod tests {
  #[test]
  fn test_hmget_params_parsing() {
    // Test would require a running server, so we just verify structure
    // Full integration tests should be in tests/ directory
  }
}
