//! EXISTS command implementation
//!
//! EXISTS key [key ...]
//! Returns the number of keys that exist from those specified as arguments.

use crate::encoding::{HashMetadata, StringValue};
use crate::error::{CoreDbError, CoreDbResult, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

/// EXISTS command parameters
#[derive(Debug, Clone, PartialEq)]
pub struct ExistsParams {
  pub keys: Vec<String>,
}

impl ExistsParams {
  /// Parse EXISTS command parameters from RESP array items
  /// Format: EXISTS key [key ...]
  fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
    // Need at least: EXISTS key (2 items)
    if items.len() < 2 {
      return Err(ProtocolError::WrongArgCount("exists"));
    }

    let mut keys = Vec::with_capacity(items.len() - 1);
    for item in items.iter().skip(1) {
      let key = match item {
        Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
        Value::SimpleString(s) => s.clone(),
        _ => return Err(ProtocolError::WrongArgCount("exists")),
      };
      keys.push(key);
    }

    Ok(ExistsParams { keys })
  }
}

/// Check if a key exists (handling both string and hash types with expiration)
async fn key_exists(server: &Server, key: &str) -> CoreDbResult<bool> {
  let raw_value = match server.get(key).await? {
    Some(v) => v,
    None => return Ok(false),
  };

  // Try to deserialize as HashMetadata first
  if let Ok(metadata) = HashMetadata::deserialize(&raw_value) {
    // Check if hash is expired
    if metadata.is_expired(now_ms()) {
      // Lazily delete the expired key
      let _ = server.delete(key).await;
      return Ok(false);
    }
    return Ok(true);
  }

  // Try to deserialize as StringValue
  if let Ok(string_value) = StringValue::deserialize(&raw_value) {
    // Check if string is expired
    if string_value.is_expired(now_ms()) {
      // Lazily delete the expired key
      let _ = server.delete(key).await;
      return Ok(false);
    }
    return Ok(true);
  }

  // Unknown type but key exists, count it
  Ok(true)
}

/// EXISTS command executor
pub struct ExistsCommand;

#[async_trait]
impl Command for ExistsCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    let params = ExistsParams::parse(items)?;

    let mut exists_count = 0i64;

    for key in &params.keys {
      if key_exists(server, key).await? {
        exists_count += 1;
      }
    }

    Ok(Value::Integer(exists_count))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_exists_params_parse_single_key() {
    let items = vec![
      Value::BulkString(Some(b"EXISTS".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
    ];
    let params = ExistsParams::parse(&items).unwrap();
    assert_eq!(params.keys, vec!["mykey"]);
  }

  #[test]
  fn test_exists_params_parse_multiple_keys() {
    let items = vec![
      Value::BulkString(Some(b"EXISTS".to_vec())),
      Value::BulkString(Some(b"key1".to_vec())),
      Value::BulkString(Some(b"key2".to_vec())),
      Value::BulkString(Some(b"key3".to_vec())),
    ];
    let params = ExistsParams::parse(&items).unwrap();
    assert_eq!(params.keys, vec!["key1", "key2", "key3"]);
  }

  #[test]
  fn test_exists_params_parse_duplicate_keys() {
    let items = vec![
      Value::BulkString(Some(b"EXISTS".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
    ];
    let params = ExistsParams::parse(&items).unwrap();
    assert_eq!(params.keys, vec!["mykey", "mykey"]);
  }

  #[test]
  fn test_exists_params_parse_insufficient_args() {
    let items = vec![Value::BulkString(Some(b"EXISTS".to_vec()))];
    assert!(ExistsParams::parse(&items).is_err());

    let items: Vec<Value> = vec![];
    assert!(ExistsParams::parse(&items).is_err());
  }

  #[test]
  fn test_exists_params_parse_with_simple_string() {
    let items = vec![
      Value::SimpleString("EXISTS".to_string()),
      Value::SimpleString("mykey".to_string()),
    ];
    let params = ExistsParams::parse(&items).unwrap();
    assert_eq!(params.keys, vec!["mykey"]);
  }

  #[test]
  fn test_exists_params_parse_with_mixed_types() {
    let items = vec![
      Value::BulkString(Some(b"EXISTS".to_vec())),
      Value::SimpleString("key1".to_string()),
      Value::BulkString(Some(b"key2".to_vec())),
    ];
    let params = ExistsParams::parse(&items).unwrap();
    assert_eq!(params.keys, vec!["key1", "key2"]);
  }

  #[test]
  fn test_exists_params_parse_invalid_key_type() {
    let items = vec![
      Value::BulkString(Some(b"EXISTS".to_vec())),
      Value::Integer(123),
    ];
    assert!(ExistsParams::parse(&items).is_err());
  }
}
