//! TTL command implementation
//!
//! TTL key
//! Returns the remaining time to live of a key that has a timeout.
//!
//! Return values:
//! - `-2` if the key does not exist or is expired
//! - `-1` if the key exists but has no associated expiration
//! - `>= 0` the remaining TTL in seconds

use crate::encoding::{
  BitmapMetadata, HashMetadata, JsonMetadata, ListMetadata, SetMetadata, StringValue, ZSetMetadata,
};
use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

/// TTL command parameters
#[derive(Debug, Clone, PartialEq)]
pub struct TtlParams {
  pub key: String,
}

impl TtlParams {
  /// Parse TTL command parameters from RESP array items
  /// Format: TTL key
  fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
    if items.len() != 2 {
      return Err(ProtocolError::WrongArgCount("ttl"));
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::WrongArgCount("ttl")),
    };

    Ok(TtlParams { key })
  }
}

/// Read the expiration timestamp of a key, handling expiration detection.
/// Returns:
/// - `None` if the key does not exist or is expired
/// - `Some(0)` if the key exists but has no expiration
/// - `Some(expires_at)` if the key exists and has an expiration
async fn read_expires_at(server: &Server, key: &str) -> Option<u64> {
  let raw_value = match server.get(key).await {
    Ok(Some(v)) => v,
    _ => return None,
  };

  let now = now_ms();

  // Try each known metadata type to extract expires_at
  macro_rules! try_deserialize {
    ($ty:ty) => {
      if let Ok(meta) = <$ty>::deserialize(&raw_value) {
        if meta.is_expired(now) {
          let _ = server.delete(key).await;
          return None;
        }
        return Some(meta.expires_at);
      }
    };
  }

  try_deserialize!(StringValue);
  try_deserialize!(HashMetadata);
  try_deserialize!(ListMetadata);
  try_deserialize!(SetMetadata);
  try_deserialize!(ZSetMetadata);
  try_deserialize!(BitmapMetadata);
  try_deserialize!(JsonMetadata);

  // Unknown type: key exists but we can't tell if it has TTL
  Some(0)
}

/// TTL command executor
pub struct TtlCommand;

#[async_trait]
impl Command for TtlCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    let params = TtlParams::parse(items)?;

    match read_expires_at(server, &params.key).await {
      None => Ok(Value::Integer(-2)),
      Some(0) => Ok(Value::Integer(-1)),
      Some(expires_at) => {
        let now = now_ms();
        let ttl_ms = expires_at.saturating_sub(now);
        let ttl_sec = (ttl_ms / 1000) as i64;
        Ok(Value::Integer(ttl_sec))
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_ttl_params_parse_basic() {
    let items = vec![
      Value::BulkString(Some(b"TTL".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
    ];
    let params = TtlParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
  }

  #[test]
  fn test_ttl_params_parse_insufficient_args() {
    let items = vec![Value::BulkString(Some(b"TTL".to_vec()))];
    assert!(TtlParams::parse(&items).is_err());

    let items: Vec<Value> = vec![];
    assert!(TtlParams::parse(&items).is_err());
  }

  #[test]
  fn test_ttl_params_parse_too_many_args() {
    let items = vec![
      Value::BulkString(Some(b"TTL".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"extra".to_vec())),
    ];
    assert!(TtlParams::parse(&items).is_err());
  }

  #[test]
  fn test_ttl_params_parse_with_simple_string() {
    let items = vec![
      Value::SimpleString("TTL".to_string()),
      Value::SimpleString("mykey".to_string()),
    ];
    let params = TtlParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
  }

  #[test]
  fn test_ttl_params_parse_invalid_key_type() {
    let items = vec![
      Value::BulkString(Some(b"TTL".to_vec())),
      Value::Integer(123),
    ];
    assert!(TtlParams::parse(&items).is_err());
  }
}
