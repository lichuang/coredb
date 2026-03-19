use crate::encoding::StringValue;
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

/// Parameters for MGET command
#[derive(Debug, Clone, PartialEq)]
pub struct MgetParams {
  pub keys: Vec<String>,
}

impl MgetParams {
  /// Parse MGET command parameters from RESP array items
  fn parse(items: &[Value]) -> Option<Self> {
    if items.len() < 2 {
      return None;
    }

    let mut keys = Vec::with_capacity(items.len() - 1);
    for item in &items[1..] {
      let key = match item {
        Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
        Value::SimpleString(s) => s.clone(),
        _ => return None,
      };
      keys.push(key);
    }

    Some(MgetParams { keys })
  }
}

/// Get a value from the server, checking for expiration.
/// Returns None if key not found or expired.
async fn get_value_check_expiry(server: &Server, key: &str) -> Option<Vec<u8>> {
  let raw_value = match server.get(key).await {
    Ok(Some(v)) => v,
    _ => return None,
  };

  // Deserialize and check expiration
  let string_value = match StringValue::deserialize(&raw_value) {
    Ok(v) => v,
    Err(_) => return None,
  };

  // Check if expired
  if string_value.is_expired(now_ms()) {
    // Lazily delete the expired key
    let _ = server.delete(key).await;
    return None;
  }

  Some(string_value.data)
}

/// MGET command executor
pub struct MgetCommand;

#[async_trait]
impl Command for MgetCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Value {
    let params = match MgetParams::parse(items) {
      Some(params) => params,
      None => return Value::error("ERR wrong number of arguments for 'mget' command"),
    };

    let mut results = Vec::with_capacity(params.keys.len());

    for key in &params.keys {
      match get_value_check_expiry(server, key).await {
        Some(data) => results.push(Value::BulkString(Some(data))),
        None => results.push(Value::BulkString(None)), // Key not found or expired
      }
    }

    Value::Array(Some(results))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_mget_params_parse_single_key() {
    let items = vec![
      Value::BulkString(Some(b"MGET".to_vec())),
      Value::BulkString(Some(b"key1".to_vec())),
    ];
    let params = MgetParams::parse(&items).unwrap();
    assert_eq!(params.keys, vec!["key1"]);
  }

  #[test]
  fn test_mget_params_parse_multiple_keys() {
    let items = vec![
      Value::BulkString(Some(b"MGET".to_vec())),
      Value::BulkString(Some(b"key1".to_vec())),
      Value::BulkString(Some(b"key2".to_vec())),
      Value::BulkString(Some(b"key3".to_vec())),
    ];
    let params = MgetParams::parse(&items).unwrap();
    assert_eq!(params.keys, vec!["key1", "key2", "key3"]);
  }

  #[test]
  fn test_mget_params_parse_no_keys() {
    let items = vec![Value::BulkString(Some(b"MGET".to_vec()))];
    assert!(MgetParams::parse(&items).is_none());
  }

  #[test]
  fn test_mget_params_parse_simple_string() {
    let items = vec![
      Value::SimpleString("MGET".to_string()),
      Value::SimpleString("key1".to_string()),
      Value::SimpleString("key2".to_string()),
    ];
    let params = MgetParams::parse(&items).unwrap();
    assert_eq!(params.keys, vec!["key1", "key2"]);
  }
}
