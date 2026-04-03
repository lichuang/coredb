use crate::encoding::{HashMetadata, StringValue, TYPE_HASH, TYPE_STRING};
use crate::error::{CoreDbError, ProtocolError};
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
  fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
    if items.len() != 2 {
      return Err(ProtocolError::WrongArgCount("type"));
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::WrongArgCount("type")),
    };

    Ok(TypeParams { key })
  }
}

/// TYPE command executor
pub struct TypeCommand;

#[async_trait]
impl Command for TypeCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    let params = TypeParams::parse(items)?;

    // Get the raw value from storage
    let raw_value = match server.get(&params.key).await? {
      Some(v) => v,
      None => return Ok(Value::SimpleString("none".to_string())),
    };

    // Try to parse as HashMetadata first (since hash has more fields)
    if let Ok(hash_meta) = HashMetadata::deserialize(&raw_value) {
      if hash_meta.is_expired(now_ms()) {
        return Ok(Value::SimpleString("none".to_string()));
      }
      // Check the type from flags
      match hash_meta.get_type() {
        TYPE_HASH => return Ok(Value::SimpleString("hash".to_string())),
        TYPE_STRING => return Ok(Value::SimpleString("string".to_string())),
        _ => {}
      }
    }

    // Try to parse as StringValue
    if let Ok(string_value) = StringValue::deserialize(&raw_value) {
      if string_value.is_expired(now_ms()) {
        return Ok(Value::SimpleString("none".to_string()));
      }
      // Check the type from flags
      match string_value.get_type() {
        TYPE_STRING => return Ok(Value::SimpleString("string".to_string())),
        TYPE_HASH => return Ok(Value::SimpleString("hash".to_string())),
        _ => {}
      }
    }

    // If we get here, treat it as non-existent
    Ok(Value::SimpleString("none".to_string()))
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
    assert!(TypeParams::parse(&items).is_err());
  }

  #[test]
  fn test_type_params_parse_too_many_args() {
    let items = vec![
      Value::BulkString(Some(b"TYPE".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"extra".to_vec())),
    ];
    assert!(TypeParams::parse(&items).is_err());
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
