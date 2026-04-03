//! HEXISTS command implementation
//!
//! HEXISTS key field
//! Returns whether the field exists in the hash stored at key.
//!
//! Returns:
//! - 1 if the field exists in the hash
//! - 0 if the field does not exist or the key does not exist

use crate::encoding::{HashFieldValue, HashMetadata};
use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

/// HEXISTS command handler
pub struct HExistsCommand;

#[async_trait]
impl Command for HExistsCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    // Parse HEXISTS key field
    if items.len() < 3 {
      return Err(ProtocolError::WrongArgCount("hexists").into());
    }

    // Parse key
    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::InvalidArgument("key").into()),
    };

    // Parse field
    let field = match &items[2] {
      Value::BulkString(Some(data)) => data.clone(),
      Value::SimpleString(s) => s.as_bytes().to_vec(),
      _ => return Err(ProtocolError::InvalidArgument("field").into()),
    };

    // Get metadata
    let metadata = match server.get(&key).await? {
      Some(raw_meta) => match HashMetadata::deserialize(&raw_meta) {
        Ok(meta) => {
          // Check if expired
          if meta.is_expired(now_ms()) {
            return Ok(Value::Integer(0)); // Return 0 if expired
          }
          meta
        }
        Err(_) => {
          return Ok(Value::Integer(0)); // Return 0 if corrupted
        }
      },
      None => {
        return Ok(Value::Integer(0)); // Return 0 if key not found
      }
    };

    // Check if field exists
    let version = metadata.version;
    let sub_key_str = HashFieldValue::build_sub_key_hex(key.as_bytes(), version, &field);

    match server.get(&sub_key_str).await? {
      Some(_) => Ok(Value::Integer(1)), // Field exists
      None => Ok(Value::Integer(0)),    // Field does not exist
    }
  }
}
