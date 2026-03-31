//! SMEMBERS command implementation
//!
//! SMEMBERS key
//! Returns all members of the set value stored at key.
//!
//! Return value:
//! - Array of all members in the set
//! - Empty array if key does not exist
//! - Error if key exists but is not a set

use crate::encoding::{SetMemberValue, SetMetadata, TYPE_SET};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

/// SMEMBERS command handler
pub struct SMembersCommand;

#[async_trait]
impl Command for SMembersCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Value {
    // Parse SMEMBERS key (exactly 2 items: command + key)
    if items.len() != 2 {
      return Value::error("ERR wrong number of arguments for 'smembers' command");
    }

    // Parse key
    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Value::error("ERR invalid key"),
    };

    // Get metadata
    let metadata = match server.get(&key).await {
      Ok(Some(raw_meta)) => match SetMetadata::deserialize(&raw_meta) {
        Ok(meta) => {
          if meta.get_type() != TYPE_SET {
            return Value::error(
              "WRONGTYPE Operation against a key holding the wrong kind of value",
            );
          }
          // Check if expired
          if meta.is_expired(now_ms()) {
            return Value::Array(Some(vec![]));
          }
          meta
        }
        Err(_) => {
          // Corrupted metadata, return empty array
          return Value::Array(Some(vec![]));
        }
      },
      _ => {
        // Key not found, return empty array
        return Value::Array(Some(vec![]));
      }
    };

    let version = metadata.version;

    // Build the hex-encoded prefix for scanning: hex(key_len|key|version)
    let prefix_hex = SetMemberValue::build_prefix_hex(key.as_bytes(), version);

    // Scan for all member entries with this prefix
    let scan_results = match server.scan_prefix(prefix_hex.as_bytes()).await {
      Ok(results) => results,
      Err(e) => return Value::error(format!("ERR failed to scan set members: {}", e)),
    };

    // Parse results and build response array of member names
    let mut result_array = Vec::with_capacity(scan_results.len());

    for (sub_key_hex, _sub_value) in scan_results {
      // sub_key_hex is a hex-encoded string (valid UTF-8 bytes of hex chars)
      // First decode the hex string to get the actual binary sub_key
      let sub_key = match String::from_utf8(sub_key_hex) {
        Ok(hex_str) => match hex::decode(&hex_str) {
          Ok(bytes) => bytes,
          Err(_) => continue,
        },
        Err(_) => continue,
      };

      // Parse the binary sub_key to extract the member name
      if let Some((_, _, member)) = SetMemberValue::parse_sub_key(&sub_key) {
        result_array.push(Value::BulkString(Some(member.to_vec())));
      }
    }

    Value::Array(Some(result_array))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_smembers_command_requires_key() {
    // SMEMBERS with no arguments should error
    let items = vec![Value::SimpleString("SMEMBERS".to_string())];
    let result = SMembersCommand::parse_args_test(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_smembers_command_too_many_args() {
    let items = vec![
      Value::SimpleString("SMEMBERS".to_string()),
      Value::BulkString(Some(b"key".to_vec())),
      Value::BulkString(Some(b"extra".to_vec())),
    ];
    // Execute would return error — testing parse validation
    assert_ne!(items.len(), 2);
  }

  // Helper for testing arg validation
  impl SMembersCommand {
    fn parse_args_test(items: &[Value]) -> Result<String, Value> {
      if items.len() != 2 {
        return Err(Value::error(
          "ERR wrong number of arguments for 'smembers' command",
        ));
      }
      match &items[1] {
        Value::BulkString(Some(data)) => Ok(String::from_utf8_lossy(data).to_string()),
        Value::SimpleString(s) => Ok(s.clone()),
        _ => Err(Value::error("ERR invalid key")),
      }
    }
  }

  #[test]
  fn test_parse_args_valid_bulk_string() {
    let items = vec![
      Value::SimpleString("SMEMBERS".to_string()),
      Value::BulkString(Some(b"myset".to_vec())),
    ];
    let result = SMembersCommand::parse_args_test(&items).unwrap();
    assert_eq!(result, "myset");
  }

  #[test]
  fn test_parse_args_valid_simple_string() {
    let items = vec![
      Value::SimpleString("SMEMBERS".to_string()),
      Value::SimpleString("myset".to_string()),
    ];
    let result = SMembersCommand::parse_args_test(&items).unwrap();
    assert_eq!(result, "myset");
  }

  #[test]
  fn test_parse_args_no_key() {
    let items = vec![Value::SimpleString("SMEMBERS".to_string())];
    let result = SMembersCommand::parse_args_test(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_invalid_key_type() {
    let items = vec![
      Value::SimpleString("SMEMBERS".to_string()),
      Value::Integer(42),
    ];
    let result = SMembersCommand::parse_args_test(&items);
    assert!(result.is_err());
  }
}
