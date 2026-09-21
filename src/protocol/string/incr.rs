use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::protocol::string::atomic_incr::atomic_incr;
use crate::server::Server;
use async_trait::async_trait;

/// Parameters for INCR command
#[derive(Debug, Clone, PartialEq)]
pub struct IncrParams {
  pub key: String,
}

impl IncrParams {
  fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
    if items.len() != 2 {
      return Err(ProtocolError::WrongArgCount("INCR"));
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::InvalidArgument("key")),
    };

    Ok(IncrParams { key })
  }
}

/// INCR command executor
pub struct IncrCommand;

#[async_trait]
impl Command for IncrCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    let params = IncrParams::parse(items)?;
    let new_int = atomic_incr(server, &params.key, 1).await?;
    Ok(Value::Integer(new_int))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_incr_params_parse_success() {
    let items = vec![
      Value::BulkString(Some(b"INCR".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
    ];
    let params = IncrParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
  }

  #[test]
  fn test_incr_params_parse_simple_string() {
    let items = vec![
      Value::SimpleString("INCR".to_string()),
      Value::SimpleString("mykey".to_string()),
    ];
    let params = IncrParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
  }

  #[test]
  fn test_incr_params_parse_no_key() {
    let items = vec![Value::BulkString(Some(b"INCR".to_vec()))];
    assert!(IncrParams::parse(&items).is_err());
  }

  #[test]
  fn test_incr_params_parse_too_many_args() {
    let items = vec![
      Value::BulkString(Some(b"INCR".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"extra".to_vec())),
    ];
    assert!(IncrParams::parse(&items).is_err());
  }
}
