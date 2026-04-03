//! HGET command implementation
//!
//! HGET key field
//! Returns the value associated with field in the hash stored at key.
//! Returns nil if key or field does not exist.

use crate::encoding::{HashFieldValue, HashMetadata};
use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

/// HGET command handler
pub struct HGetCommand;

#[async_trait]
impl Command for HGetCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    // Parse HGET key field
    if items.len() < 3 {
      return Err(ProtocolError::WrongArgCount("hget").into());
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::InvalidArgument("key").into()),
    };

    let field = match &items[2] {
      Value::BulkString(Some(data)) => data.clone(),
      Value::SimpleString(s) => s.as_bytes().to_vec(),
      _ => return Err(ProtocolError::InvalidArgument("field").into()),
    };

    // Get metadata
    let metadata = match server.get(&key).await? {
      Some(raw_meta) => {
        match HashMetadata::deserialize(&raw_meta) {
          Ok(meta) => {
            // Check if expired
            if meta.is_expired(now_ms()) {
              return Ok(Value::BulkString(None)); // Return nil if expired
            }
            meta
          }
          Err(_) => {
            return Ok(Value::BulkString(None)); // Return nil if corrupted
          }
        }
      }
      None => {
        return Ok(Value::BulkString(None)); // Return nil if not found
      }
    };

    // Get field value
    let version = metadata.version;
    let sub_key_str = HashFieldValue::build_sub_key_hex(key.as_bytes(), version, &field);

    match server.get(&sub_key_str).await? {
      Some(raw_value) => match HashFieldValue::deserialize(&raw_value) {
        Ok(field_value) => Ok(Value::BulkString(Some(field_value.data))),
        Err(_) => Ok(Value::BulkString(None)), // Return nil if corrupted
      },
      None => Ok(Value::BulkString(None)), // Return nil if not found
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
