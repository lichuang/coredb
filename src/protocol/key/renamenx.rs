//! RENAMENX command implementation
//!
//! RENAMENX key newkey
//!
//! Renames `key` to `newkey` only if `newkey` does not exist.
//! Returns 1 if renamed, 0 if newkey already exists.
//! Returns an error if the source key does not exist.

use crate::encoding::{
  BitmapMetadata, BloomFilterMetadata, HashMetadata, HyperLogLogMetadata, JsonMetadata,
  ListMetadata, SetMetadata, StringValue, TYPE_BITMAP, TYPE_BLOOMFILTER, TYPE_HASH,
  TYPE_HYPERLOGLOG, TYPE_JSON, TYPE_LIST, TYPE_SET, TYPE_STRING, TYPE_ZSET, ZSetMetadata,
};
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

    // NX check: if destination exists and is not expired, return 0
    if let Some(dest_raw) = server.get(&params.new_key).await? {
      if check_dest_exists(&dest_raw, now) {
        return Ok(Value::Integer(0));
      }
      // Destination is expired — clean it up before proceeding
      delete_dest_if_complex(server, &params.new_key, &dest_raw, now).await?;
      let _ = server.delete(&params.new_key).await;
    }

    // Type dispatch — same as RENAME but with Integer(1) success value

    // Attempt StringValue
    if let Ok(sv) = StringValue::deserialize(&raw_value) {
      if sv.is_expired(now) {
        let _ = server.delete(&params.key).await;
        return Err(ProtocolError::Custom("ERR no such key").into());
      }
      if sv.get_type() == TYPE_STRING {
        server.set(params.new_key, sv.serialize()).await?;
        server.delete(&params.key).await?;
        return Ok(Value::Integer(1));
      }
    }

    // Attempt HashMetadata
    if let Ok(hm) = HashMetadata::deserialize(&raw_value) {
      if hm.is_expired(now) {
        let _ = server.delete(&params.key).await;
        return Err(ProtocolError::Custom("ERR no such key").into());
      }
      if hm.get_type() == TYPE_HASH {
        return rename_complex_type(
          server,
          &params.key,
          &params.new_key,
          hm.version,
          &raw_value,
          now,
          Value::Integer(1),
        )
        .await;
      }
    }

    // Attempt ListMetadata
    if let Ok(lm) = ListMetadata::deserialize(&raw_value) {
      if lm.is_expired(now) {
        let _ = server.delete(&params.key).await;
        return Err(ProtocolError::Custom("ERR no such key").into());
      }
      if lm.get_type() == TYPE_LIST {
        return rename_complex_type(
          server,
          &params.key,
          &params.new_key,
          lm.version,
          &raw_value,
          now,
          Value::Integer(1),
        )
        .await;
      }
    }

    // Attempt SetMetadata
    if let Ok(sm) = SetMetadata::deserialize(&raw_value) {
      if sm.is_expired(now) {
        let _ = server.delete(&params.key).await;
        return Err(ProtocolError::Custom("ERR no such key").into());
      }
      if sm.get_type() == TYPE_SET {
        return rename_complex_type(
          server,
          &params.key,
          &params.new_key,
          sm.version,
          &raw_value,
          now,
          Value::Integer(1),
        )
        .await;
      }
    }

    // Attempt ZSetMetadata
    if let Ok(zm) = ZSetMetadata::deserialize(&raw_value) {
      if zm.is_expired(now) {
        let _ = server.delete(&params.key).await;
        return Err(ProtocolError::Custom("ERR no such key").into());
      }
      if zm.get_type() == TYPE_ZSET {
        return rename_complex_type(
          server,
          &params.key,
          &params.new_key,
          zm.version,
          &raw_value,
          now,
          Value::Integer(1),
        )
        .await;
      }
    }

    // Attempt BitmapMetadata
    if let Ok(bm) = BitmapMetadata::deserialize(&raw_value) {
      if bm.is_expired(now) {
        let _ = server.delete(&params.key).await;
        return Err(ProtocolError::Custom("ERR no such key").into());
      }
      if bm.get_type() == TYPE_BITMAP {
        return rename_complex_type(
          server,
          &params.key,
          &params.new_key,
          bm.version,
          &raw_value,
          now,
          Value::Integer(1),
        )
        .await;
      }
    }

    // Attempt JsonMetadata (simple type — single key, like String)
    if let Ok(jm) = JsonMetadata::deserialize(&raw_value) {
      if jm.is_expired(now) {
        let _ = server.delete(&params.key).await;
        return Err(ProtocolError::Custom("ERR no such key").into());
      }
      if jm.get_type() == TYPE_JSON {
        server.set(params.new_key, raw_value).await?;
        server.delete(&params.key).await?;
        return Ok(Value::Integer(1));
      }
    }

    // Attempt BloomFilterMetadata (complex type — metadata + sub-keys)
    if let Ok(bf) = BloomFilterMetadata::deserialize(&raw_value) {
      if bf.is_expired(now) {
        let _ = server.delete(&params.key).await;
        return Err(ProtocolError::Custom("ERR no such key").into());
      }
      if bf.get_type() == TYPE_BLOOMFILTER {
        return rename_complex_type(
          server,
          &params.key,
          &params.new_key,
          bf.version,
          &raw_value,
          now,
          Value::Integer(1),
        )
        .await;
      }
    }

    // Attempt HyperLogLogMetadata (complex type — metadata + segments)
    if let Ok(hll) = HyperLogLogMetadata::deserialize(&raw_value) {
      if hll.is_expired(now) {
        let _ = server.delete(&params.key).await;
        return Err(ProtocolError::Custom("ERR no such key").into());
      }
      if hll.get_type() == TYPE_HYPERLOGLOG {
        return rename_complex_type(
          server,
          &params.key,
          &params.new_key,
          hll.version,
          &raw_value,
          now,
          Value::Integer(1),
        )
        .await;
      }
    }

    // Unknown type — try a generic rename (just move the raw bytes)
    server.set(params.new_key, raw_value).await?;
    server.delete(&params.key).await?;
    Ok(Value::Integer(1))
  }
}

/// Check if a destination key value is valid (exists and not expired).
fn check_dest_exists(raw: &[u8], now: u64) -> bool {
  if let Ok(sv) = StringValue::deserialize(raw)
    && sv.get_type() == TYPE_STRING
  {
    return !sv.is_expired(now);
  }
  if let Ok(hm) = HashMetadata::deserialize(raw)
    && hm.get_type() == TYPE_HASH
  {
    return !hm.is_expired(now);
  }
  if let Ok(lm) = ListMetadata::deserialize(raw)
    && lm.get_type() == TYPE_LIST
  {
    return !lm.is_expired(now);
  }
  if let Ok(sm) = SetMetadata::deserialize(raw)
    && sm.get_type() == TYPE_SET
  {
    return !sm.is_expired(now);
  }
  if let Ok(zm) = ZSetMetadata::deserialize(raw)
    && zm.get_type() == TYPE_ZSET
  {
    return !zm.is_expired(now);
  }
  if let Ok(bm) = BitmapMetadata::deserialize(raw)
    && bm.get_type() == TYPE_BITMAP
  {
    return !bm.is_expired(now);
  }
  if let Ok(jm) = JsonMetadata::deserialize(raw)
    && jm.get_type() == TYPE_JSON
  {
    return !jm.is_expired(now);
  }
  if let Ok(bf) = BloomFilterMetadata::deserialize(raw)
    && bf.get_type() == TYPE_BLOOMFILTER
  {
    return !bf.is_expired(now);
  }
  if let Ok(hll) = HyperLogLogMetadata::deserialize(raw)
    && hll.get_type() == TYPE_HYPERLOGLOG
  {
    return !hll.is_expired(now);
  }
  // Can't deserialize — treat as existing (conservative)
  true
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
    let sv = StringValue::new(b"hello");
    let raw = sv.serialize();
    assert!(check_dest_exists(&raw, now_ms()));
  }

  #[test]
  fn test_check_dest_exists_expired_string() {
    let sv = StringValue::with_expiration(b"hello", 1); // expired
    let raw = sv.serialize();
    assert!(!check_dest_exists(&raw, now_ms()));
  }
}
