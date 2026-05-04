//! RENAME command implementation
//!
//! RENAME key newkey
//!
//! Renames `key` to `newkey`. If `newkey` already exists it is overwritten.
//! Returns an error if the source key does not exist.

use rockraft::raft::types::UpsertKV;

use crate::encoding::{
  BitmapMetadata, BloomFilterMetadata, HashMetadata, HyperLogLogMetadata, JsonMetadata,
  ListMetadata, SetMetadata, StringValue, TYPE_BITMAP, TYPE_BLOOMFILTER, TYPE_HASH,
  TYPE_HYPERLOGLOG, TYPE_JSON, TYPE_LIST, TYPE_SET, TYPE_STRING, TYPE_ZSET, ZSetMetadata,
};
use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

/// RENAME command parameters
#[derive(Debug, Clone, PartialEq)]
pub struct RenameParams {
  pub key: String,
  pub new_key: String,
}

impl RenameParams {
  /// Parse RENAME command parameters from RESP array items
  /// Format: RENAME key newkey
  fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
    if items.len() != 3 {
      return Err(ProtocolError::WrongArgCount("rename"));
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::WrongArgCount("rename")),
    };

    let new_key = match &items[2] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::WrongArgCount("rename")),
    };

    Ok(RenameParams { key, new_key })
  }
}

/// Build the hex-encoded prefix for scanning sub-keys of a complex type
/// Format: hex(key_len(4 bytes, BE) | key | version(8 bytes, BE))
pub fn build_prefix_hex(key: &[u8], version: u64) -> String {
  let key_len = key.len() as u32;
  let mut prefix = Vec::with_capacity(4 + key.len() + 8);
  prefix.extend_from_slice(&key_len.to_be_bytes());
  prefix.extend_from_slice(key);
  prefix.extend_from_slice(&version.to_be_bytes());
  hex::encode(&prefix)
}

/// Delete all sub-keys and the metadata key for a complex type at `key`.
/// Used to clean up the destination before overwriting.
pub async fn delete_complex_key(
  server: &Server,
  key: &str,
  version: u64,
) -> Result<(), CoreDbError> {
  let prefix = build_prefix_hex(key.as_bytes(), version);
  let scan_results = server.scan_prefix(prefix.as_bytes()).await?;
  for (sub_key, _) in scan_results {
    let sub_key_str = String::from_utf8_lossy(&sub_key);
    let _ = server.delete(&sub_key_str).await;
  }
  let _ = server.delete(key).await;
  Ok(())
}

/// RENAME command executor
pub struct RenameCommand;

#[async_trait]
impl Command for RenameCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    let params = RenameParams::parse(items)?;

    // Same-key rename is a no-op success
    if params.key == params.new_key {
      return Ok(Value::SimpleString("OK".to_string()));
    }

    // Read source key
    let raw_value = match server.get(&params.key).await? {
      Some(v) => v,
      None => {
        return Err(ProtocolError::Custom("ERR no such key").into());
      }
    };

    let now = now_ms();

    // Try to determine the type by examining the flags byte (low 4 bits)
    // All encoding types share the same flags layout: high 4 bits = version, low 4 bits = type
    // We need to figure out the data type. Try HashMetadata first (it's the most common complex type
    // and can overlap with StringValue in postcard encoding).

    // Attempt StringValue
    if let Ok(sv) = StringValue::deserialize(&raw_value) {
      if sv.is_expired(now) {
        let _ = server.delete(&params.key).await;
        return Err(ProtocolError::Custom("ERR no such key").into());
      }
      if sv.get_type() == TYPE_STRING {
        // Simple type: just copy to new key and delete old key
        // First, delete destination if it exists (and is complex type)
        if let Some(dest_raw) = server.get(&params.new_key).await? {
          delete_dest_if_complex(server, &params.new_key, &dest_raw, now).await?;
        }
        server.set(params.new_key, sv.serialize()).await?;
        server.delete(&params.key).await?;
        return Ok(Value::SimpleString("OK".to_string()));
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
          Value::SimpleString("OK".to_string()),
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
          Value::SimpleString("OK".to_string()),
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
          Value::SimpleString("OK".to_string()),
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
          Value::SimpleString("OK".to_string()),
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
          Value::SimpleString("OK".to_string()),
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
        if let Some(dest_raw) = server.get(&params.new_key).await? {
          delete_dest_if_complex(server, &params.new_key, &dest_raw, now).await?;
        }
        server.set(params.new_key, raw_value).await?;
        server.delete(&params.key).await?;
        return Ok(Value::SimpleString("OK".to_string()));
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
          Value::SimpleString("OK".to_string()),
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
          Value::SimpleString("OK".to_string()),
        )
        .await;
      }
    }

    // Unknown type — try a generic rename (just move the raw bytes)
    // First clean up destination if it's complex
    if let Some(dest_raw) = server.get(&params.new_key).await? {
      delete_dest_if_complex(server, &params.new_key, &dest_raw, now).await?;
    }
    server.set(params.new_key, raw_value).await?;
    server.delete(&params.key).await?;
    Ok(Value::SimpleString("OK".to_string()))
  }
}

/// If the destination key exists and is a complex type, delete its sub-keys.
pub async fn delete_dest_if_complex(
  server: &Server,
  dest_key: &str,
  dest_raw: &[u8],
  now: u64,
) -> Result<(), CoreDbError> {
  // Try each complex metadata type to see if destination has sub-keys to clean up
  if let Ok(hm) = HashMetadata::deserialize(dest_raw)
    && !hm.is_expired(now)
    && hm.get_type() == TYPE_HASH
  {
    delete_complex_key(server, dest_key, hm.version).await?;
    return Ok(());
  }
  if let Ok(lm) = ListMetadata::deserialize(dest_raw)
    && !lm.is_expired(now)
    && lm.get_type() == TYPE_LIST
  {
    delete_complex_key(server, dest_key, lm.version).await?;
    return Ok(());
  }
  if let Ok(sm) = SetMetadata::deserialize(dest_raw)
    && !sm.is_expired(now)
    && sm.get_type() == TYPE_SET
  {
    delete_complex_key(server, dest_key, sm.version).await?;
    return Ok(());
  }
  if let Ok(zm) = ZSetMetadata::deserialize(dest_raw)
    && !zm.is_expired(now)
    && zm.get_type() == TYPE_ZSET
  {
    delete_complex_key(server, dest_key, zm.version).await?;
    return Ok(());
  }
  if let Ok(bm) = BitmapMetadata::deserialize(dest_raw)
    && !bm.is_expired(now)
    && bm.get_type() == TYPE_BITMAP
  {
    delete_complex_key(server, dest_key, bm.version).await?;
    return Ok(());
  }
  if let Ok(bf) = BloomFilterMetadata::deserialize(dest_raw)
    && !bf.is_expired(now)
    && bf.get_type() == TYPE_BLOOMFILTER
  {
    delete_complex_key(server, dest_key, bf.version).await?;
    return Ok(());
  }
  if let Ok(hll) = HyperLogLogMetadata::deserialize(dest_raw)
    && !hll.is_expired(now)
    && hll.get_type() == TYPE_HYPERLOGLOG
  {
    delete_complex_key(server, dest_key, hll.version).await?;
    return Ok(());
  }
  // Simple type or unknown — no sub-keys to clean up
  Ok(())
}

/// Rename a complex type: scan source sub-keys, recreate at destination with same version,
/// delete source sub-keys, write destination metadata, delete source metadata.
pub async fn rename_complex_type(
  server: &Server,
  src_key: &str,
  dst_key: &str,
  src_version: u64,
  src_metadata_raw: &[u8],
  now: u64,
  success_value: Value,
) -> Result<Value, CoreDbError> {
  // 1. If destination already exists, clean it up first
  if let Some(dest_raw) = server.get(dst_key).await? {
    delete_dest_if_complex(server, dst_key, &dest_raw, now).await?;
  }

  // 2. Scan all source sub-keys
  let src_prefix = build_prefix_hex(src_key.as_bytes(), src_version);
  let scan_results = server.scan_prefix(src_prefix.as_bytes()).await?;

  // 3. Build batch write entries: insert all sub-keys under new key name (same version)
  let mut entries: Vec<UpsertKV> = Vec::with_capacity(scan_results.len() + 2);

  // Parse each source sub-key, rebuild with destination key
  for (sub_key_hex_bytes, sub_value) in &scan_results {
    let sub_key_hex = String::from_utf8_lossy(sub_key_hex_bytes);
    let sub_key_bin = match hex::decode(sub_key_hex.as_ref()) {
      Ok(b) => b,
      Err(_) => continue,
    };

    // Parse to extract the trailing part (field / member / index etc.)
    // The format is: key_len(4) | key | version(8) | trailing_part
    if sub_key_bin.len() < 12 {
      continue;
    }
    let key_len = u32::from_be_bytes([
      sub_key_bin[0],
      sub_key_bin[1],
      sub_key_bin[2],
      sub_key_bin[3],
    ]) as usize;
    if sub_key_bin.len() < 12 + key_len {
      continue;
    }
    let trailing = &sub_key_bin[4 + key_len + 8..];

    // Rebuild sub-key with destination key, same version
    let new_sub_key_bin = build_sub_key_with_new_key(dst_key.as_bytes(), src_version, trailing);
    let new_sub_key_hex = hex::encode(&new_sub_key_bin);

    // Insert at destination
    entries.push(UpsertKV::insert(&new_sub_key_hex, sub_value));

    // Delete from source
    entries.push(UpsertKV::delete(&*sub_key_hex));
  }

  // 4. Delete old metadata and insert new metadata (move the raw metadata bytes)
  entries.push(UpsertKV::delete(src_key));
  entries.push(UpsertKV::insert(dst_key, src_metadata_raw));

  // 5. Atomic batch write
  server.batch_write(entries).await?;

  Ok(success_value)
}

/// Build a sub-key binary: key_len(4 BE) | new_key | version(8 BE) | trailing
pub fn build_sub_key_with_new_key(new_key: &[u8], version: u64, trailing: &[u8]) -> Vec<u8> {
  let key_len = new_key.len() as u32;
  let mut buf = Vec::with_capacity(4 + new_key.len() + 8 + trailing.len());
  buf.extend_from_slice(&key_len.to_be_bytes());
  buf.extend_from_slice(new_key);
  buf.extend_from_slice(&version.to_be_bytes());
  buf.extend_from_slice(trailing);
  buf
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_rename_params_parse_success() {
    let items = vec![
      Value::BulkString(Some(b"RENAME".to_vec())),
      Value::BulkString(Some(b"oldkey".to_vec())),
      Value::BulkString(Some(b"newkey".to_vec())),
    ];
    let params = RenameParams::parse(&items).unwrap();
    assert_eq!(params.key, "oldkey");
    assert_eq!(params.new_key, "newkey");
  }

  #[test]
  fn test_rename_params_parse_simple_string() {
    let items = vec![
      Value::SimpleString("RENAME".to_string()),
      Value::SimpleString("oldkey".to_string()),
      Value::SimpleString("newkey".to_string()),
    ];
    let params = RenameParams::parse(&items).unwrap();
    assert_eq!(params.key, "oldkey");
    assert_eq!(params.new_key, "newkey");
  }

  #[test]
  fn test_rename_params_parse_insufficient_args() {
    let items = vec![Value::BulkString(Some(b"RENAME".to_vec()))];
    assert!(RenameParams::parse(&items).is_err());

    let items = vec![
      Value::BulkString(Some(b"RENAME".to_vec())),
      Value::BulkString(Some(b"key".to_vec())),
    ];
    assert!(RenameParams::parse(&items).is_err());

    let items: Vec<Value> = vec![];
    assert!(RenameParams::parse(&items).is_err());
  }

  #[test]
  fn test_rename_params_parse_too_many_args() {
    let items = vec![
      Value::BulkString(Some(b"RENAME".to_vec())),
      Value::BulkString(Some(b"key".to_vec())),
      Value::BulkString(Some(b"newkey".to_vec())),
      Value::BulkString(Some(b"extra".to_vec())),
    ];
    assert!(RenameParams::parse(&items).is_err());
  }

  #[test]
  fn test_rename_params_parse_invalid_key_type() {
    let items = vec![
      Value::BulkString(Some(b"RENAME".to_vec())),
      Value::Integer(123),
      Value::BulkString(Some(b"newkey".to_vec())),
    ];
    assert!(RenameParams::parse(&items).is_err());

    let items = vec![
      Value::BulkString(Some(b"RENAME".to_vec())),
      Value::BulkString(Some(b"key".to_vec())),
      Value::Integer(456),
    ];
    assert!(RenameParams::parse(&items).is_err());
  }

  #[test]
  fn test_build_prefix_hex() {
    let key = b"mykey";
    let version = 12345u64;

    let prefix = build_prefix_hex(key, version);

    // Decode and verify structure: key_len(4) | key | version(8)
    let bytes = hex::decode(&prefix).unwrap();
    assert_eq!(bytes.len(), 4 + 5 + 8);

    let key_len = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    assert_eq!(key_len, 5);
    assert_eq!(&bytes[4..9], b"mykey");
    assert_eq!(&bytes[9..17], &version.to_be_bytes());
  }

  #[test]
  fn test_build_sub_key_with_new_key() {
    let new_key = b"dest";
    let version = 999u64;
    let trailing = b"myfield";

    let result = build_sub_key_with_new_key(new_key, version, trailing);

    // Verify: key_len(4) | new_key | version(8) | trailing
    assert_eq!(result.len(), 4 + 4 + 8 + 7);

    let key_len = u32::from_be_bytes([result[0], result[1], result[2], result[3]]) as usize;
    assert_eq!(key_len, 4);
    assert_eq!(&result[4..8], b"dest");
    assert_eq!(&result[8..16], &version.to_be_bytes());
    assert_eq!(&result[16..], b"myfield");
  }

  #[test]
  fn test_build_sub_key_with_new_key_empty_trailing() {
    let new_key = b"dest";
    let version = 0u64;
    let trailing = b"";

    let result = build_sub_key_with_new_key(new_key, version, trailing);
    assert_eq!(result.len(), 4 + 4 + 8);
  }
}
