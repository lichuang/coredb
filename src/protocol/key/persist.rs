//! PERSIST command implementation
//!
//! PERSIST key
//!
//! Remove the existing timeout on key, turning the key from volatile
//! (a key with an expire set) to persistent (a key that will never expire
//! as no timeout is associated).
//!
//! Return values:
//! - `1` if the timeout was removed
//! - `0` if the key does not exist or does not have an associated timeout

use crate::encoding::NO_EXPIRATION;
use crate::encoding::{
  BitmapMetadata, BloomFilterMetadata, HashMetadata, HyperLogLogMetadata, JsonMetadata,
  ListMetadata, SetMetadata, StringValue, ZSetMetadata,
};
use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

/// PERSIST command parameters
#[derive(Debug, Clone, PartialEq)]
pub struct PersistParams {
  pub key: String,
}

impl PersistParams {
  /// Parse PERSIST command parameters from RESP array items
  /// Format: PERSIST key
  fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
    if items.len() != 2 {
      return Err(ProtocolError::WrongArgCount("persist"));
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::WrongArgCount("persist")),
    };

    Ok(PersistParams { key })
  }
}

/// PERSIST command executor
pub struct PersistCommand;

#[async_trait]
impl Command for PersistCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    let params = PersistParams::parse(items)?;

    // Read raw value from store
    let raw_value = match server.get(&params.key).await? {
      Some(v) => v,
      None => return Ok(Value::Boolean(false)),
    };

    let now = now_ms();

    macro_rules! try_persist {
      ($ty:ty) => {
        if let Ok(mut meta) = <$ty>::deserialize(&raw_value) {
          if meta.is_expired(now) {
            let _ = server.delete(&params.key).await;
            return Ok(Value::Boolean(false));
          }
          if meta.expires_at == NO_EXPIRATION {
            return Ok(Value::Boolean(false));
          }
          meta.expires_at = NO_EXPIRATION;
          server.set(params.key.clone(), meta.serialize()).await?;
          return Ok(Value::Boolean(true));
        }
      };
    }

    try_persist!(StringValue);
    try_persist!(HashMetadata);
    try_persist!(ListMetadata);
    try_persist!(SetMetadata);
    try_persist!(ZSetMetadata);
    try_persist!(BitmapMetadata);
    try_persist!(BloomFilterMetadata);
    try_persist!(HyperLogLogMetadata);
    try_persist!(JsonMetadata);

    // Unknown type
    Ok(Value::Boolean(false))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_persist_params_parse_basic() {
    let items = vec![
      Value::BulkString(Some(b"PERSIST".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
    ];
    let params = PersistParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
  }

  #[test]
  fn test_persist_params_parse_insufficient_args() {
    let items = vec![Value::BulkString(Some(b"PERSIST".to_vec()))];
    assert!(PersistParams::parse(&items).is_err());

    let items: Vec<Value> = vec![];
    assert!(PersistParams::parse(&items).is_err());
  }

  #[test]
  fn test_persist_params_parse_too_many_args() {
    let items = vec![
      Value::BulkString(Some(b"PERSIST".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"extra".to_vec())),
    ];
    assert!(PersistParams::parse(&items).is_err());
  }

  #[test]
  fn test_persist_params_parse_with_simple_string() {
    let items = vec![
      Value::SimpleString("PERSIST".to_string()),
      Value::SimpleString("mykey".to_string()),
    ];
    let params = PersistParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
  }

  #[test]
  fn test_persist_params_parse_invalid_key_type() {
    let items = vec![
      Value::BulkString(Some(b"PERSIST".to_vec())),
      Value::Integer(123),
    ];
    assert!(PersistParams::parse(&items).is_err());
  }
}
