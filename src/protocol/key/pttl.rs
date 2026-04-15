//! PTTL command implementation
//!
//! PTTL key
//! Returns the remaining time to live of a key that has a timeout, in milliseconds.
//!
//! Return values:
//! - `-2` if the key does not exist or is expired
//! - `-1` if the key exists but has no associated expiration
//! - `>= 0` the remaining TTL in milliseconds

use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::key::ttl::read_expires_at;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

/// PTTL command parameters
#[derive(Debug, Clone, PartialEq)]
pub struct PttlParams {
  pub key: String,
}

impl PttlParams {
  /// Parse PTTL command parameters from RESP array items
  /// Format: PTTL key
  fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
    if items.len() != 2 {
      return Err(ProtocolError::WrongArgCount("pttl"));
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::WrongArgCount("pttl")),
    };

    Ok(PttlParams { key })
  }
}

/// PTTL command executor
pub struct PttlCommand;

#[async_trait]
impl Command for PttlCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    let params = PttlParams::parse(items)?;

    match read_expires_at(server, &params.key).await {
      None => Ok(Value::Integer(-2)),
      Some(0) => Ok(Value::Integer(-1)),
      Some(expires_at) => {
        let now = now_ms();
        let ttl_ms = (expires_at as i64).saturating_sub(now as i64);
        Ok(Value::Integer(ttl_ms))
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_pttl_params_parse_basic() {
    let items = vec![
      Value::BulkString(Some(b"PTTL".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
    ];
    let params = PttlParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
  }

  #[test]
  fn test_pttl_params_parse_insufficient_args() {
    let items = vec![Value::BulkString(Some(b"PTTL".to_vec()))];
    assert!(PttlParams::parse(&items).is_err());

    let items: Vec<Value> = vec![];
    assert!(PttlParams::parse(&items).is_err());
  }

  #[test]
  fn test_pttl_params_parse_too_many_args() {
    let items = vec![
      Value::BulkString(Some(b"PTTL".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"extra".to_vec())),
    ];
    assert!(PttlParams::parse(&items).is_err());
  }

  #[test]
  fn test_pttl_params_parse_with_simple_string() {
    let items = vec![
      Value::SimpleString("PTTL".to_string()),
      Value::SimpleString("mykey".to_string()),
    ];
    let params = PttlParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
  }

  #[test]
  fn test_pttl_params_parse_invalid_key_type() {
    let items = vec![
      Value::BulkString(Some(b"PTTL".to_vec())),
      Value::Integer(123),
    ];
    assert!(PttlParams::parse(&items).is_err());
  }
}
