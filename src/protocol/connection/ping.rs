//! PING command implementation
//!
//! PING [message]
//! Returns PONG if no argument is provided, otherwise returns the message.

use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use async_trait::async_trait;

/// PING command handler
pub struct PingCommand;

#[async_trait]
impl Command for PingCommand {
  async fn execute(&self, items: &[Value], _server: &Server) -> Result<Value, CoreDbError> {
    // PING can have 0 or 1 argument
    // PING -> PONG
    // PING message -> message

    if items.len() > 2 {
      return Err(ProtocolError::WrongArgCount("ping").into());
    }

    if items.len() == 1 {
      // No argument, return PONG
      Ok(Value::SimpleString("PONG".to_string()))
    } else {
      // Return the provided message
      match &items[1] {
        Value::BulkString(Some(data)) => Ok(Value::BulkString(Some(data.clone()))),
        Value::BulkString(None) => Ok(Value::BulkString(None)),
        Value::SimpleString(s) => Ok(Value::SimpleString(s.clone())),
        Value::Integer(i) => Ok(Value::Integer(*i)),
        Value::Array(_) => Err(ProtocolError::InvalidArgument("argument type").into()),
        Value::Error(e) => Ok(Value::Error(e.clone())),
      }
    }
  }
}
