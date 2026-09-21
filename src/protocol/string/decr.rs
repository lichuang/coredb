use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::protocol::string::atomic_incr::atomic_incr;
use crate::server::Server;
use async_trait::async_trait;

/// Parameters for DECR command
#[derive(Debug, Clone, PartialEq)]
pub struct DecrParams {
  pub key: String,
}

impl DecrParams {
  fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
    if items.len() != 2 {
      return Err(ProtocolError::WrongArgCount("DECR"));
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::InvalidArgument("key")),
    };

    Ok(DecrParams { key })
  }
}

/// DECR command executor
pub struct DecrCommand;

#[async_trait]
impl Command for DecrCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    let params = DecrParams::parse(items)?;
    let new_int = atomic_incr(server, &params.key, -1).await?;
    Ok(Value::Integer(new_int))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_decr_params_parse_success() {
    let items = vec![
      Value::BulkString(Some(b"DECR".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
    ];
    let params = DecrParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
  }

  #[test]
  fn test_decr_params_parse_simple_string() {
    let items = vec![
      Value::SimpleString("DECR".to_string()),
      Value::SimpleString("mykey".to_string()),
    ];
    let params = DecrParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
  }

  #[test]
  fn test_decr_params_parse_no_key() {
    let items = vec![Value::BulkString(Some(b"DECR".to_vec()))];
    assert!(DecrParams::parse(&items).is_err());
  }

  #[test]
  fn test_decr_params_parse_too_many_args() {
    let items = vec![
      Value::BulkString(Some(b"DECR".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"extra".to_vec())),
    ];
    assert!(DecrParams::parse(&items).is_err());
  }
}
