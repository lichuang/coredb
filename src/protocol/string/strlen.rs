use crate::encoding::StringValue;
use crate::error::{CoreDbError, CoreDbResult, ProtocolError};
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
  fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
    if items.len() != 2 {
      return Err(ProtocolError::WrongArgCount("STRLEN"));
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::InvalidArgument("key")),
    };

    Ok(StrlenParams { key })
  }
}

/// Get a value from the server, checking for expiration.
/// Returns None if key not found or expired.
async fn get_value_check_expiry(server: &Server, key: &str) -> CoreDbResult<Option<Vec<u8>>> {
  let raw_value = match server.get(key).await? {
    Some(v) => v,
    None => return Ok(None),
  };

  let string_value = match StringValue::deserialize(&raw_value) {
    Ok(v) => v,
    Err(_) => return Err(crate::error::ProtocolError::WrongType.into()),
  };

  if string_value.is_expired(now_ms()) {
    let _ = server.delete(key).await;
    return Ok(None);
  }

  Ok(Some(string_value.data))
}

/// STRLEN command executor
pub struct StrlenCommand;

#[async_trait]
impl Command for StrlenCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    let params = StrlenParams::parse(items)?;

    match get_value_check_expiry(server, &params.key).await? {
      Some(data) => Ok(Value::Integer(data.len() as i64)),
      None => Ok(Value::Integer(0)),
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
    let items = vec![Value::BulkString(Some(b"STRLEN".to_vec()))];
    assert!(StrlenParams::parse(&items).is_err());

    let items = vec![
      Value::BulkString(Some(b"STRLEN".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"extra".to_vec())),
    ];
    assert!(StrlenParams::parse(&items).is_err());
  }

  #[test]
  fn test_strlen_params_parse_invalid_key_type() {
    let items = vec![
      Value::BulkString(Some(b"STRLEN".to_vec())),
      Value::Integer(123),
    ];
    assert!(StrlenParams::parse(&items).is_err());
  }
}
