//! DEL command implementation
//!
//! DEL key [key ...]
//! Removes the specified keys. A key is ignored if it does not exist.
//!
//! Returns:
//! - The number of keys that were removed
//! - 0 if none of the specified keys existed

use crate::encoding::{HashMetadata, StringValue};
use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

/// DEL command parameters
#[derive(Debug, Clone, PartialEq)]
pub struct DelParams {
  pub keys: Vec<String>,
}

impl DelParams {
  /// Parse DEL command parameters from RESP array items
  /// Format: DEL key [key ...]
  fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
    // Need at least: DEL key (2 items)
    if items.len() < 2 {
      return Err(ProtocolError::WrongArgCount("del"));
    }

    let mut keys = Vec::with_capacity(items.len() - 1);
    for item in items.iter().skip(1) {
      let key = match item {
        Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
        Value::SimpleString(s) => s.clone(),
        _ => return Err(ProtocolError::WrongArgCount("del")),
      };
      keys.push(key);
    }

    Ok(DelParams { keys })
  }
}

/// DEL command executor
pub struct DelCommand;

#[async_trait]
impl Command for DelCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    let params = DelParams::parse(items)?;

    let mut deleted_count = 0i64;

    for key in params.keys {
      // Try to get the value first to determine its type
      let raw_value = match server.get(&key).await? {
        Some(v) => v,
        None => continue, // Key doesn't exist, skip
      };

      // Check if it's a Hash type by trying to deserialize as HashMetadata
      if let Ok(metadata) = HashMetadata::deserialize(&raw_value) {
        // Check if hash is expired
        if !metadata.is_expired(now_ms()) {
          // It's a valid hash, delete all its fields first
          let prefix = format!("{}|{:08x}", hex::encode(key.as_bytes()), metadata.version);
          if let Ok(fields) = server.scan_prefix(prefix.as_bytes()).await {
            for (field_key, _) in fields {
              let field_key_str = String::from_utf8_lossy(&field_key);
              let _ = server.delete(&field_key_str).await;
            }
          }
          deleted_count += 1;
        }
        // Delete the metadata key
        let _ = server.delete(&key).await;
      } else if StringValue::deserialize(&raw_value).is_ok() {
        // It's a string value, delete it directly
        if server.delete(&key).await? {
          deleted_count += 1;
        }
      } else {
        // Unknown type, try to delete anyway
        if server.delete(&key).await? {
          deleted_count += 1;
        }
      }
    }

    Ok(Value::Integer(deleted_count))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_del_params_parse_single_key() {
    let items = vec![
      Value::BulkString(Some(b"DEL".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
    ];
    let params = DelParams::parse(&items).unwrap();
    assert_eq!(params.keys, vec!["mykey"]);
  }

  #[test]
  fn test_del_params_parse_multiple_keys() {
    let items = vec![
      Value::BulkString(Some(b"DEL".to_vec())),
      Value::BulkString(Some(b"key1".to_vec())),
      Value::BulkString(Some(b"key2".to_vec())),
      Value::BulkString(Some(b"key3".to_vec())),
    ];
    let params = DelParams::parse(&items).unwrap();
    assert_eq!(params.keys, vec!["key1", "key2", "key3"]);
  }

  #[test]
  fn test_del_params_parse_insufficient_args() {
    // Only DEL command, no keys
    let items = vec![Value::BulkString(Some(b"DEL".to_vec()))];
    assert!(DelParams::parse(&items).is_err());

    // Empty items
    let items: Vec<Value> = vec![];
    assert!(DelParams::parse(&items).is_err());
  }

  #[test]
  fn test_del_params_parse_with_simple_string() {
    let items = vec![
      Value::SimpleString("DEL".to_string()),
      Value::SimpleString("mykey".to_string()),
    ];
    let params = DelParams::parse(&items).unwrap();
    assert_eq!(params.keys, vec!["mykey"]);
  }

  #[test]
  fn test_del_params_parse_with_mixed_types() {
    let items = vec![
      Value::BulkString(Some(b"DEL".to_vec())),
      Value::SimpleString("key1".to_string()),
      Value::BulkString(Some(b"key2".to_vec())),
    ];
    let params = DelParams::parse(&items).unwrap();
    assert_eq!(params.keys, vec!["key1", "key2"]);
  }

  #[test]
  fn test_del_params_parse_invalid_key_type() {
    let items = vec![
      Value::BulkString(Some(b"DEL".to_vec())),
      Value::Integer(123),
    ];
    assert!(DelParams::parse(&items).is_err());
  }
}
