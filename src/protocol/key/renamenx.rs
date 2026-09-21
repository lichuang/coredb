//! RENAMENX command implementation
//!
//! RENAMENX key newkey
//!
//! Renames `key` to `newkey` only if `newkey` does not exist.
//! Returns 1 if renamed, 0 if newkey already exists.
//! Returns an error if the source key does not exist.

use crate::encoding::ValueMeta;
use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::key::rename::{delete_dest_if_complex, rename_complex_type};
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

/// RENAMENX command parameters
#[derive(Debug, Clone, PartialEq)]
pub struct RenameNxParams {
  pub key: String,
  pub new_key: String,
}

impl RenameNxParams {
  /// Parse RENAMENX command parameters from RESP array items
  /// Format: RENAMENX key newkey
  fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
    if items.len() != 3 {
      return Err(ProtocolError::WrongArgCount("renamenx"));
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::WrongArgCount("renamenx")),
    };

    let new_key = match &items[2] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::WrongArgCount("renamenx")),
    };

    Ok(RenameNxParams { key, new_key })
  }
}

/// RENAMENX command executor
pub struct RenameNxCommand;

#[async_trait]
impl Command for RenameNxCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    let params = RenameNxParams::parse(items)?;

    // Same-key: newkey exists (= src), return 0
    if params.key == params.new_key {
      return Ok(Value::Integer(0));
    }

    // Read source key
    let raw_value = match server.get(&params.key).await? {
      Some(v) => v,
      None => {
        return Err(ProtocolError::Custom("ERR no such key").into());
      }
    };

    let now = now_ms();

    if let Some(dest_raw) = server.get(&params.new_key).await? {
      if check_dest_exists(&dest_raw, now) {
        return Ok(Value::Integer(0));
      }
      delete_dest_if_complex(server, &params.new_key, &dest_raw, now).await?;
      let _ = server.delete(&params.new_key).await;
    }

    let meta = match ValueMeta::decode(&raw_value) {
      Some(meta) if meta.is_expired(now) => {
        let _ = server.delete(&params.key).await;
        return Err(ProtocolError::Custom("ERR no such key").into());
      }
      Some(meta) => meta,
      None => return Err(ProtocolError::Custom("ERR no such key").into()),
    };

    if meta.kind.is_complex() {
      return rename_complex_type(
        server,
        &params.key,
        &params.new_key,
        meta.version,
        &raw_value,
        now,
        Value::Integer(1),
      )
      .await;
    }

    server.set(params.new_key, raw_value).await?;
    server.delete(&params.key).await?;
    Ok(Value::Integer(1))
  }
}

/// Check if a destination key value is valid (exists and not expired).
fn check_dest_exists(raw: &[u8], now: u64) -> bool {
  match ValueMeta::decode(raw) {
    Some(meta) => !meta.is_expired(now),
    None => true,
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_renamenx_params_parse_success() {
    let items = vec![
      Value::BulkString(Some(b"RENAMENX".to_vec())),
      Value::BulkString(Some(b"oldkey".to_vec())),
      Value::BulkString(Some(b"newkey".to_vec())),
    ];
    let params = RenameNxParams::parse(&items).unwrap();
    assert_eq!(params.key, "oldkey");
    assert_eq!(params.new_key, "newkey");
  }

  #[test]
  fn test_renamenx_params_parse_simple_string() {
    let items = vec![
      Value::SimpleString("RENAMENX".to_string()),
      Value::SimpleString("oldkey".to_string()),
      Value::SimpleString("newkey".to_string()),
    ];
    let params = RenameNxParams::parse(&items).unwrap();
    assert_eq!(params.key, "oldkey");
    assert_eq!(params.new_key, "newkey");
  }

  #[test]
  fn test_renamenx_params_parse_insufficient_args() {
    let items = vec![Value::BulkString(Some(b"RENAMENX".to_vec()))];
    assert!(RenameNxParams::parse(&items).is_err());

    let items = vec![
      Value::BulkString(Some(b"RENAMENX".to_vec())),
      Value::BulkString(Some(b"key".to_vec())),
    ];
    assert!(RenameNxParams::parse(&items).is_err());

    let items: Vec<Value> = vec![];
    assert!(RenameNxParams::parse(&items).is_err());
  }

  #[test]
  fn test_renamenx_params_parse_too_many_args() {
    let items = vec![
      Value::BulkString(Some(b"RENAMENX".to_vec())),
      Value::BulkString(Some(b"key".to_vec())),
      Value::BulkString(Some(b"newkey".to_vec())),
      Value::BulkString(Some(b"extra".to_vec())),
    ];
    assert!(RenameNxParams::parse(&items).is_err());
  }

  #[test]
  fn test_renamenx_params_parse_invalid_key_type() {
    let items = vec![
      Value::BulkString(Some(b"RENAMENX".to_vec())),
      Value::Integer(123),
      Value::BulkString(Some(b"newkey".to_vec())),
    ];
    assert!(RenameNxParams::parse(&items).is_err());

    let items = vec![
      Value::BulkString(Some(b"RENAMENX".to_vec())),
      Value::BulkString(Some(b"key".to_vec())),
      Value::Integer(456),
    ];
    assert!(RenameNxParams::parse(&items).is_err());
  }

  #[test]
  fn test_check_dest_exists_valid_string() {
    let raw = crate::encoding::StringValue::new(b"hello").serialize();
    assert!(check_dest_exists(&raw, now_ms()));
    assert!(check_dest_exists(&raw, u64::MAX));
  }

  #[test]
  fn test_check_dest_exists_expired_string() {
    let raw = crate::encoding::StringValue::with_expiration(b"hello", 1).serialize();
    assert!(!check_dest_exists(&raw, now_ms()));
  }

  #[test]
  fn test_check_dest_exists_undecodable_is_conservative() {
    assert!(check_dest_exists(&[0x0F, 0x00], now_ms()));
  }
}
