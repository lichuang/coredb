use crate::encoding::StringValue;
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

/// Parameters for STRLEN command
#[derive(Debug, Clone, PartialEq)]
pub struct StrlenParams {
  pub key: String,
}

impl StrlenParams {
  /// Parse STRLEN command parameters from RESP array items
  fn parse(items: &[Value]) -> Option<Self> {
    if items.len() != 2 {
      return None;
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return None,
    };

    Some(StrlenParams { key })
  }
}

/// Get a value from the server, checking for expiration.
/// Returns (value, expired) where `expired` is true if the key was expired and deleted.
async fn get_value_check_expiry(server: &Server, key: &str) -> Result<Option<Vec<u8>>, String> {
  let raw_value = match server.get(key).await? {
    Some(v) => v,
    None => return Ok(None),
  };

  // Deserialize and check expiration
  let string_value = match StringValue::deserialize(&raw_value) {
    Ok(v) => v,
    Err(_) => {
      return Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string());
    }
  };

  // Check if expired
  if string_value.is_expired(now_ms()) {
    // Lazily delete the expired key
    let _ = server.delete(key).await;
    return Ok(None);
  }

  Ok(Some(string_value.data))
}

/// STRLEN command executor
pub struct StrlenCommand;

#[async_trait]
impl Command for StrlenCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Value {
    let params = match StrlenParams::parse(items) {
      Some(params) => params,
      None => return Value::error("ERR wrong number of arguments for 'strlen' command"),
    };

    match get_value_check_expiry(server, &params.key).await {
      Ok(Some(data)) => Value::Integer(data.len() as i64),
      Ok(None) => Value::Integer(0), // Key not found or expired
      Err(e) => {
        // Check if it's a WRONGTYPE error (don't prefix with ERR)
        if e.starts_with("WRONGTYPE") {
          Value::error(e)
        } else {
          Value::error(format!("ERR {}", e))
        }
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_strlen_params_parse_success() {
    let items = vec![
      Value::BulkString(Some(b"STRLEN".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
    ];
    let params = StrlenParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
  }

  #[test]
  fn test_strlen_params_parse_simple_string() {
    let items = vec![
      Value::SimpleString("STRLEN".to_string()),
      Value::SimpleString("mykey".to_string()),
    ];
    let params = StrlenParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
  }

  #[test]
  fn test_strlen_params_parse_wrong_args() {
    // Too few arguments
    let items = vec![Value::BulkString(Some(b"STRLEN".to_vec()))];
    assert!(StrlenParams::parse(&items).is_none());

    // Too many arguments
    let items = vec![
      Value::BulkString(Some(b"STRLEN".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"extra".to_vec())),
    ];
    assert!(StrlenParams::parse(&items).is_none());
  }

  #[test]
  fn test_strlen_params_parse_invalid_key_type() {
    let items = vec![
      Value::BulkString(Some(b"STRLEN".to_vec())),
      Value::Integer(123),
    ];
    assert!(StrlenParams::parse(&items).is_none());
  }
}
