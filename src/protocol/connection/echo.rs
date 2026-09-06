//! ECHO command implementation
//!
//! ECHO message
//! Returns the given message.

use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use async_trait::async_trait;

/// ECHO command handler
pub struct EchoCommand;

impl EchoCommand {
  /// Build the response value for a single message argument.
  fn reply(message: &Value) -> Result<Value, ProtocolError> {
    match message {
      Value::BulkString(Some(data)) => Ok(Value::BulkString(Some(data.clone()))),
      Value::BulkString(None) => Ok(Value::BulkString(None)),
      Value::SimpleString(s) => Ok(Value::SimpleString(s.clone())),
      Value::Integer(i) => Ok(Value::Integer(*i)),
      _ => Err(ProtocolError::InvalidArgument("message")),
    }
  }
}

#[async_trait]
impl Command for EchoCommand {
  async fn execute(&self, items: &[Value], _server: &Server) -> Result<Value, CoreDbError> {
    if items.len() != 2 {
      return Err(ProtocolError::WrongArgCount("echo").into());
    }
    Ok(Self::reply(&items[1])?)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_reply_bulk_string() {
    let result = EchoCommand::reply(&Value::BulkString(Some(b"hello".to_vec()))).unwrap();
    assert_eq!(result, Value::BulkString(Some(b"hello".to_vec())));
  }

  #[test]
  fn test_reply_empty_bulk_string() {
    let result = EchoCommand::reply(&Value::BulkString(Some(b"".to_vec()))).unwrap();
    assert_eq!(result, Value::BulkString(Some(b"".to_vec())));
  }

  #[test]
  fn test_reply_nil_bulk_string() {
    let result = EchoCommand::reply(&Value::BulkString(None)).unwrap();
    assert_eq!(result, Value::BulkString(None));
  }

  #[test]
  fn test_reply_simple_string() {
    let result = EchoCommand::reply(&Value::SimpleString("hello".to_string())).unwrap();
    assert_eq!(result, Value::SimpleString("hello".to_string()));
  }

  #[test]
  fn test_reply_integer() {
    let result = EchoCommand::reply(&Value::Integer(42)).unwrap();
    assert_eq!(result, Value::Integer(42));
  }

  #[test]
  fn test_reply_rejects_array() {
    let result = EchoCommand::reply(&Value::Array(Some(vec![])));
    assert!(result.is_err());
  }

  #[test]
  fn test_reply_rejects_map() {
    let result = EchoCommand::reply(&Value::Map(vec![]));
    assert!(result.is_err());
  }
}
