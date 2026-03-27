use crate::encoding::StringValue;
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

/// Parameters for SETNX command
///
/// Standard Redis SETNX command format:
/// SETNX key value
#[derive(Debug, Clone, PartialEq)]
pub struct SetnxParams {
  /// The key to set
  pub key: String,
  /// The value to set
  pub value: Vec<u8>,
}

impl SetnxParams {
  /// Create a new SetnxParams with minimal required fields
  pub fn new(key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
    Self {
      key: key.into(),
      value: value.into(),
    }
  }

  /// Parse SETNX command parameters from RESP array items
  /// Format: SETNX key value
  fn parse(items: &[Value]) -> Option<Self> {
    // SETNX requires exactly 3 items: SETNX key value
    if items.len() != 3 {
      return None;
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return None,
    };

    let value = match &items[2] {
      Value::BulkString(Some(data)) => data.clone(),
      Value::SimpleString(s) => s.as_bytes().to_vec(),
      _ => return None,
    };

    Some(SetnxParams::new(key, value))
  }
}

/// SETNX command executor
pub struct SetnxCommand;

#[async_trait]
impl Command for SetnxCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Value {
    let params = match SetnxParams::parse(items) {
      Some(params) => params,
      None => return Value::error("ERR wrong number of arguments for 'setnx' command"),
    };

    // Get current timestamp for expiration check
    let now = now_ms();

    // Check if key exists
    match server.get(&params.key).await {
      Ok(Some(raw_value)) => {
        // Key exists - check if it's expired or a valid string
        match StringValue::deserialize(&raw_value) {
          Ok(value) if !value.is_expired(now) => {
            // Key exists and is not expired, do not set
            return Value::Integer(0);
          }
          Ok(_) => {
            // Key is expired, treat as not exists - fall through to set
          }
          Err(_) => {
            // Not a StringValue (might be Hash or other type) - key exists
            return Value::Integer(0);
          }
        }
      }
      Ok(None) => {
        // Key doesn't exist - fall through to set
      }
      Err(e) => {
        return Value::error(format!("ERR {}", e));
      }
    }

    // Key doesn't exist (or is expired), create new StringValue without expiration
    let string_value = StringValue::new(params.value);
    let serialized = string_value.serialize();

    // Set the new value
    match server.set(params.key, serialized).await {
      Ok(_) => Value::Integer(1),
      Err(e) => Value::error(format!("ERR {}", e)),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_setnx_params_parse_basic() {
    let items = vec![
      Value::BulkString(Some(b"SETNX".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
    ];
    let params = SetnxParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
    assert_eq!(params.value, b"myvalue");
  }

  #[test]
  fn test_setnx_params_parse_simple_string() {
    let items = vec![
      Value::SimpleString("SETNX".to_string()),
      Value::SimpleString("mykey".to_string()),
      Value::SimpleString("myvalue".to_string()),
    ];
    let params = SetnxParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
    assert_eq!(params.value, b"myvalue");
  }

  #[test]
  fn test_setnx_params_parse_too_few_args() {
    let items = vec![
      Value::BulkString(Some(b"SETNX".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
    ];
    assert!(SetnxParams::parse(&items).is_none());
  }

  #[test]
  fn test_setnx_params_parse_too_many_args() {
    let items = vec![
      Value::BulkString(Some(b"SETNX".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
      Value::BulkString(Some(b"extra".to_vec())),
    ];
    assert!(SetnxParams::parse(&items).is_none());
  }

  #[test]
  fn test_setnx_params_parse_no_args() {
    let items = vec![Value::BulkString(Some(b"SETNX".to_vec()))];
    assert!(SetnxParams::parse(&items).is_none());
  }

  #[test]
  fn test_setnx_params_parse_empty_key() {
    let items = vec![
      Value::BulkString(Some(b"SETNX".to_vec())),
      Value::BulkString(Some(b"".to_vec())),
      Value::BulkString(Some(b"value".to_vec())),
    ];
    let params = SetnxParams::parse(&items).unwrap();
    assert_eq!(params.key, "");
    assert_eq!(params.value, b"value");
  }

  #[test]
  fn test_setnx_params_parse_empty_value() {
    let items = vec![
      Value::BulkString(Some(b"SETNX".to_vec())),
      Value::BulkString(Some(b"key".to_vec())),
      Value::BulkString(Some(b"".to_vec())),
    ];
    let params = SetnxParams::parse(&items).unwrap();
    assert_eq!(params.key, "key");
    assert_eq!(params.value, b"");
  }

  #[test]
  fn test_setnx_params_parse_binary_value() {
    let binary_data: Vec<u8> = vec![0x00, 0xFF, 0x80, 0x7F, 0x01, 0xFE];
    let items = vec![
      Value::BulkString(Some(b"SETNX".to_vec())),
      Value::BulkString(Some(b"binary_key".to_vec())),
      Value::BulkString(Some(binary_data.clone())),
    ];
    let params = SetnxParams::parse(&items).unwrap();
    assert_eq!(params.key, "binary_key");
    assert_eq!(params.value, binary_data);
  }

  #[test]
  fn test_setnx_params_parse_unicode_key() {
    let items = vec![
      Value::BulkString(Some(b"SETNX".to_vec())),
      Value::BulkString(Some("键".as_bytes().to_vec())),
      Value::BulkString(Some(b"value".to_vec())),
    ];
    let params = SetnxParams::parse(&items).unwrap();
    assert_eq!(params.key, "键");
    assert_eq!(params.value, b"value");
  }

  #[test]
  fn test_setnx_params_parse_unicode_value() {
    let items = vec![
      Value::BulkString(Some(b"SETNX".to_vec())),
      Value::BulkString(Some(b"key".to_vec())),
      Value::BulkString(Some("值🎉".as_bytes().to_vec())),
    ];
    let params = SetnxParams::parse(&items).unwrap();
    assert_eq!(params.key, "key");
    assert_eq!(params.value, "值🎉".as_bytes());
  }

  #[test]
  fn test_setnx_params_parse_large_value() {
    // Test with a larger value (1KB)
    let large_value: Vec<u8> = (0..1024).map(|i| (i % 256) as u8).collect();
    let items = vec![
      Value::BulkString(Some(b"SETNX".to_vec())),
      Value::BulkString(Some(b"large_key".to_vec())),
      Value::BulkString(Some(large_value.clone())),
    ];
    let params = SetnxParams::parse(&items).unwrap();
    assert_eq!(params.key, "large_key");
    assert_eq!(params.value, large_value);
  }
}
