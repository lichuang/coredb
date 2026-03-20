use crate::encoding::{HashMetadata, StringValue, TYPE_HASH, TYPE_STRING};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

/// Parameters for TYPE command
#[derive(Debug, Clone, PartialEq)]
pub struct TypeParams {
  pub key: String,
}

impl TypeParams {
  fn parse(items: &[Value]) -> Option<Self> {
    if items.len() != 2 {
      return None;
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return None,
    };

    Some(TypeParams { key })
  }
}

/// TYPE command executor
pub struct TypeCommand;

#[async_trait]
impl Command for TypeCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Value {
    let params = match TypeParams::parse(items) {
      Some(params) => params,
      None => return Value::error("ERR wrong number of arguments for 'type' command"),
    };

    // Get the raw value from storage
    let raw_value = match server.get(&params.key).await {
      Ok(Some(v)) => v,
      Ok(None) => return Value::SimpleString("none".to_string()),
      Err(e) => return Value::error(format!("ERR {}", e)),
    };

    // Try to parse as HashMetadata first (since hash has more fields)
    if let Ok(hash_meta) = HashMetadata::deserialize(&raw_value) {
      if hash_meta.is_expired(now_ms()) {
        return Value::SimpleString("none".to_string());
      }
      // Check the type from flags
      match hash_meta.get_type() {
        TYPE_HASH => return Value::SimpleString("hash".to_string()),
        TYPE_STRING => return Value::SimpleString("string".to_string()),
        _ => {}
      }
    }

    // Try to parse as StringValue
    if let Ok(string_value) = StringValue::deserialize(&raw_value) {
      if string_value.is_expired(now_ms()) {
        return Value::SimpleString("none".to_string());
      }
      // Check the type from flags
      match string_value.get_type() {
        TYPE_STRING => return Value::SimpleString("string".to_string()),
        TYPE_HASH => return Value::SimpleString("hash".to_string()),
        _ => {}
      }
    }

    // If we get here, treat it as non-existent
    Value::SimpleString("none".to_string())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_type_params_parse_success() {
    let items = vec![
      Value::BulkString(Some(b"TYPE".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
    ];
    let params = TypeParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
  }

  #[test]
  fn test_type_params_parse_missing_key() {
    let items = vec![Value::BulkString(Some(b"TYPE".to_vec()))];
    assert!(TypeParams::parse(&items).is_none());
  }

  #[test]
  fn test_type_params_parse_too_many_args() {
    let items = vec![
      Value::BulkString(Some(b"TYPE".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"extra".to_vec())),
    ];
    assert!(TypeParams::parse(&items).is_none());
  }

  #[test]
  fn test_type_params_parse_simple_string() {
    let items = vec![
      Value::SimpleString("TYPE".to_string()),
      Value::SimpleString("mykey".to_string()),
    ];
    let params = TypeParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
  }
}
