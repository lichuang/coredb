use crate::encoding::{NO_EXPIRATION, StringValue};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

/// Parameters for APPEND command
///
/// Standard Redis APPEND command format:
/// APPEND key value
#[derive(Debug, Clone, PartialEq)]
pub struct AppendParams {
  /// The key to append to
  pub key: String,
  /// The value to append
  pub value: Vec<u8>,
}

impl AppendParams {
  /// Create a new AppendParams with minimal required fields
  pub fn new(key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
    Self {
      key: key.into(),
      value: value.into(),
    }
  }

  /// Parse APPEND command parameters from RESP array items
  /// Format: APPEND key value
  fn parse(items: &[Value]) -> Option<Self> {
    // Minimum: APPEND key value
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

    Some(AppendParams::new(key, value))
  }
}

/// APPEND command executor
pub struct AppendCommand;

#[async_trait]
impl Command for AppendCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Value {
    let params = match AppendParams::parse(items) {
      Some(params) => params,
      None => return Value::error("ERR wrong number of arguments for 'append' command"),
    };

    let now = now_ms();

    // Check if key exists and get its current value
    match server.get(&params.key).await {
      Ok(Some(raw_value)) => {
        // Key exists - try to deserialize as string
        match StringValue::deserialize(&raw_value) {
          Ok(mut string_value) => {
            // Check if expired
            if string_value.is_expired(now) {
              // Key is expired - treat as new key
              let new_value = StringValue::new(params.value);
              let serialized = new_value.serialize();
              let len = new_value.data.len() as i64;

              match server.set(params.key, serialized).await {
                Ok(_) => Value::Integer(len),
                Err(e) => Value::error(format!("ERR {}", e)),
              }
            } else {
              // Key is valid string - append to it
              // Preserve expiration if it exists
              let expires_at = string_value.expires_at;
              string_value.data.extend_from_slice(&params.value);
              let len = string_value.data.len() as i64;

              let serialized = if expires_at == NO_EXPIRATION {
                StringValue::new(string_value.data)
              } else {
                StringValue::with_expiration(string_value.data, expires_at)
              }
              .serialize();

              match server.set(params.key, serialized).await {
                Ok(_) => Value::Integer(len),
                Err(e) => Value::error(format!("ERR {}", e)),
              }
            }
          }
          Err(_) => {
            // Key exists but is not a string type (e.g., hash)
            Value::error("WRONGTYPE Operation against a key holding the wrong kind of value")
          }
        }
      }
      Ok(None) => {
        // Key doesn't exist - create new key with the value
        let new_value = StringValue::new(params.value);
        let serialized = new_value.serialize();
        let len = new_value.data.len() as i64;

        match server.set(params.key, serialized).await {
          Ok(_) => Value::Integer(len),
          Err(e) => Value::error(format!("ERR {}", e)),
        }
      }
      Err(e) => Value::error(format!("ERR {}", e)),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_append_params_parse_basic() {
    let items = vec![
      Value::BulkString(Some(b"APPEND".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
    ];
    let params = AppendParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
    assert_eq!(params.value, b"myvalue");
  }

  #[test]
  fn test_append_params_parse_with_simple_string() {
    let items = vec![
      Value::SimpleString("APPEND".to_string()),
      Value::SimpleString("mykey".to_string()),
      Value::SimpleString("myvalue".to_string()),
    ];
    let params = AppendParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
    assert_eq!(params.value, b"myvalue");
  }

  #[test]
  fn test_append_params_parse_empty_value() {
    let items = vec![
      Value::BulkString(Some(b"APPEND".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"".to_vec())),
    ];
    let params = AppendParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
    assert!(params.value.is_empty());
  }

  #[test]
  fn test_append_params_parse_binary_value() {
    let binary_data = vec![0x00, 0xFF, 0xAB, 0xCD];
    let items = vec![
      Value::BulkString(Some(b"APPEND".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(binary_data.clone())),
    ];
    let params = AppendParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
    assert_eq!(params.value, binary_data);
  }

  #[test]
  fn test_append_params_parse_missing_key() {
    let items = vec![Value::BulkString(Some(b"APPEND".to_vec()))];
    assert!(AppendParams::parse(&items).is_none());
  }

  #[test]
  fn test_append_params_parse_missing_value() {
    let items = vec![
      Value::BulkString(Some(b"APPEND".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
    ];
    assert!(AppendParams::parse(&items).is_none());
  }

  #[test]
  fn test_append_params_parse_too_many_args() {
    let items = vec![
      Value::BulkString(Some(b"APPEND".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
      Value::BulkString(Some(b"extra".to_vec())),
    ];
    assert!(AppendParams::parse(&items).is_none());
  }

  #[test]
  fn test_append_params_parse_invalid_key_type() {
    let items = vec![
      Value::BulkString(Some(b"APPEND".to_vec())),
      Value::Integer(123),
      Value::BulkString(Some(b"myvalue".to_vec())),
    ];
    assert!(AppendParams::parse(&items).is_none());
  }

  #[test]
  fn test_append_params_parse_invalid_value_type() {
    let items = vec![
      Value::BulkString(Some(b"APPEND".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::Integer(123),
    ];
    assert!(AppendParams::parse(&items).is_none());
  }
}
