//! DEL command implementation
//!
//! DEL key [key ...]
//! Removes the specified keys. A key is ignored if it does not exist.
//!
//! Returns:
//! - The number of keys that were removed
//! - 0 if none of the specified keys existed

use crate::encoding::{ValueMeta, is_expired};
use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::key::rename::delete_complex_key;
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

    let now = now_ms();
    for key in params.keys {
      let raw_value = match server.get(&key).await? {
        Some(v) => v,
        None => continue,
      };

      if is_expired(&raw_value, now) {
        let _ = server.delete(&key).await;
        continue;
      }

      match ValueMeta::decode(&raw_value) {
        Some(meta) if meta.kind.is_complex() => {
          delete_complex_key(server, &key, meta.version).await?
        }
        _ => {
          server.delete(&key).await?;
        }
      }
      deleted_count += 1;
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
