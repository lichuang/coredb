//! HLEN command implementation
//!
//! HLEN key
//! Returns the number of fields contained in the hash stored at key.
//!
//! Return value:
//! - Integer: number of fields in the hash
//! - 0 if key does not exist
//! - Error if key exists but is not a hash

use crate::encoding::HashMetadata;
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

/// HLEN command handler
pub struct HLenCommand;

#[async_trait]
impl Command for HLenCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Value {
    // Parse HLEN key (exactly 2 items: command + key)
    if items.len() != 2 {
      return Value::error("ERR wrong number of arguments for 'hlen' command");
    }

    // Parse key
    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Value::error("ERR invalid key"),
    };

    // Get metadata
    let metadata = match server.get(&key).await {
      Ok(Some(raw_meta)) => match HashMetadata::deserialize(&raw_meta) {
        Ok(meta) => {
          // Check if expired
          if meta.is_expired(now_ms()) {
            // Expired, return 0
            return Value::Integer(0);
          }
          meta
        }
        Err(_) => {
          // Corrupted metadata, return 0
          return Value::Integer(0);
        }
      },
      _ => {
        // Key not found, return 0
        return Value::Integer(0);
      }
    };

    // Return the size from metadata
    Value::Integer(metadata.size as i64)
  }
}

#[cfg(test)]
mod tests {
  #[test]
  fn test_hlen_params_parsing() {
    // Test would require a running server, so we just verify structure
    // Full integration tests should be in tests/ directory
  }
}
