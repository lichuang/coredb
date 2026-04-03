//! HKEYS command implementation
//!
//! HKEYS key
//! Returns all field names in the hash stored at key.
//!
//! Return value:
//! - Array of field names
//! - Empty array if key does not exist
//! - Error if key exists but is not a hash

use crate::encoding::{HashFieldValue, HashMetadata};
use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

/// HKEYS command handler
pub struct HKeysCommand;

#[async_trait]
impl Command for HKeysCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    // Parse HKEYS key (exactly 2 items: command + key)
    if items.len() != 2 {
      return Err(ProtocolError::WrongArgCount("hkeys").into());
    }

    // Parse key
    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::InvalidArgument("key").into()),
    };

    // Get metadata
    let metadata = match server.get(&key).await? {
      Some(raw_meta) => match HashMetadata::deserialize(&raw_meta) {
        Ok(meta) => {
          // Check if expired
          if meta.is_expired(now_ms()) {
            // Expired, return empty array
            return Ok(Value::Array(Some(vec![])));
          }
          meta
        }
        Err(_) => {
          // Corrupted metadata, return empty array
          return Ok(Value::Array(Some(vec![])));
        }
      },
      None => {
        // Key not found, return empty array
        return Ok(Value::Array(Some(vec![])));
      }
    };

    let version = metadata.version;

    // Build the hex-encoded prefix for scanning: hex(key_len|key|version)
    // All hash sub-keys are stored with hex encoding, so we scan with hex prefix
    let prefix_hex = build_field_prefix_hex(key.as_bytes(), version);

    // Scan for all field-value pairs with this prefix (forwarded to leader)
    // Note: scan_prefix returns hex-encoded keys, we need to decode them
    let scan_results = server.scan_prefix(prefix_hex.as_bytes()).await?;

    // Parse results and build response array with only field names
    let mut result_array = Vec::with_capacity(scan_results.len());

    for (sub_key_hex, sub_value) in scan_results {
      // sub_key_hex is a hex-encoded string (valid UTF-8 bytes of hex chars)
      // First decode the hex string to get the actual binary sub_key
      let sub_key = match String::from_utf8(sub_key_hex) {
        Ok(hex_str) => match hex::decode(&hex_str) {
          Ok(bytes) => bytes,
          Err(_) => continue,
        },
        Err(_) => continue,
      };

      // Parse the binary sub_key to extract the field name
      if let Some((_, _, field)) = HashFieldValue::parse_sub_key(&sub_key) {
        // Deserialize the field value to ensure it's valid
        if HashFieldValue::deserialize(&sub_value).is_ok() {
          // Add field name to result array
          result_array.push(Value::BulkString(Some(field.to_vec())));
        }
      }
    }

    Ok(Value::Array(Some(result_array)))
  }
}

/// Build the hex-encoded prefix for scanning hash fields
/// Format: hex(key_len(4 bytes) | key | version(8 bytes))
fn build_field_prefix_hex(key: &[u8], version: u64) -> String {
  let key_len = key.len() as u32;
  let mut prefix = Vec::with_capacity(4 + key.len() + 8);
  prefix.extend_from_slice(&key_len.to_be_bytes());
  prefix.extend_from_slice(key);
  prefix.extend_from_slice(&version.to_be_bytes());
  hex::encode(&prefix)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_build_field_prefix_hex() {
    let key = b"myhash";
    let version = 12345u64;

    let prefix_hex = build_field_prefix_hex(key, version);

    // Verify it's valid hex
    let prefix_bytes = hex::decode(&prefix_hex).unwrap();

    // Verify structure: key_len(4) | key | version(8)
    let key_len = u32::from_be_bytes([
      prefix_bytes[0],
      prefix_bytes[1],
      prefix_bytes[2],
      prefix_bytes[3],
    ]) as usize;
    assert_eq!(key_len, key.len());
    assert_eq!(&prefix_bytes[4..4 + key_len], key);
    assert_eq!(
      &prefix_bytes[4 + key_len..4 + key_len + 8],
      &version.to_be_bytes()
    );
    assert_eq!(prefix_bytes.len(), 4 + key.len() + 8);

    // Verify hex encoding (each byte -> 2 hex chars)
    assert_eq!(prefix_hex.len(), prefix_bytes.len() * 2);
  }

  #[test]
  fn test_build_field_prefix_hex_empty_key() {
    let key = b"";
    let version = 0u64;

    let prefix_hex = build_field_prefix_hex(key, version);

    // Decode and verify
    let prefix_bytes = hex::decode(&prefix_hex).unwrap();

    // Should be: 0 (4 bytes) + empty key + version (8 bytes) = 12 bytes
    assert_eq!(prefix_bytes.len(), 12);
    let key_len = u32::from_be_bytes([
      prefix_bytes[0],
      prefix_bytes[1],
      prefix_bytes[2],
      prefix_bytes[3],
    ]);
    assert_eq!(key_len, 0);
  }

  #[test]
  fn test_build_field_prefix_hex_binary_key() {
    let key = vec![0x00, 0x01, 0xff, 0xfe];
    let version = 0xdeadbeefcafeu64;

    let prefix_hex = build_field_prefix_hex(&key, version);

    // Decode and verify
    let prefix_bytes = hex::decode(&prefix_hex).unwrap();

    let key_len = u32::from_be_bytes([
      prefix_bytes[0],
      prefix_bytes[1],
      prefix_bytes[2],
      prefix_bytes[3],
    ]) as usize;
    assert_eq!(key_len, key.len());
    assert_eq!(&prefix_bytes[4..4 + key_len], &key[..]);
  }

  #[test]
  fn test_build_field_prefix_hex_is_valid_utf8() {
    // Hex encoding produces valid UTF-8 (ASCII hex chars)
    let key = b"test";
    let version = 42u64;

    let prefix_hex = build_field_prefix_hex(key, version);

    // Should be valid ASCII/UTF-8
    assert!(prefix_hex.is_ascii());
    assert!(String::from_utf8(prefix_hex.into_bytes()).is_ok());
  }
}
