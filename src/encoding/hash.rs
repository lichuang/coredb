//! Hash type encoding/decoding for storage
//!
//! Hash data is stored in two parts:
//! 1. Metadata: stored at `key`, contains flags, expires_at, version, size
//! 2. Field-Value pairs: stored at `key|version|field`
//!
//! This design allows efficient operations on large hashes without
//! loading all fields into memory.
//!
//! # Storage Layout
//!
//! ## Hash Metadata
//! ```text
//! +-----------+------------+-----------+-----------+
//! |   flags   | expires_at |  version  |   size    |
//! | (1byte)   | (optional) |  (8byte)  |  (8byte)  |
//! +-----------+------------+-----------+-----------+
//! ```
//!
//! - `flags`: high 4 bits = encoding version, low 4 bits = data type
//! - `expires_at`: optional expiration timestamp in milliseconds
//! - `version`: used for fast deletion of large hashes
//! - `size`: number of fields in this hash
//!
//! ## Hash Field-Value
//! ```text
//!                       +---------------+
//! key|version|field => |     value     |
//!                       +---------------+
//! ```

use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::encoding::{CURRENT_VERSION, NO_EXPIRATION};

/// Hash metadata structure for storage
///
/// This struct represents the metadata of a hash key, which is stored
/// separately from the actual field-value pairs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HashMetadata {
  /// Flags field: high 4 bits = encoding version, low 4 bits = data type
  pub flags: u8,
  /// Expiration timestamp in milliseconds (Unix timestamp), 0 means no expiration
  pub expires_at: u64,
  /// Version for fast deletion (incremented on each recreation)
  pub version: u64,
  /// Number of fields in this hash
  pub size: u64,
}

impl HashMetadata {
  /// Create a new HashMetadata without expiration
  pub fn new() -> Self {
    Self {
      flags: CURRENT_VERSION,
      expires_at: NO_EXPIRATION,
      version: Self::generate_version(),
      size: 0,
    }
  }

  /// Create a new HashMetadata with expiration timestamp (in milliseconds)
  #[allow(dead_code)]
  pub fn with_expiration(expires_at: u64) -> Self {
    Self {
      flags: CURRENT_VERSION,
      expires_at,
      version: Self::generate_version(),
      size: 0,
    }
  }

  /// Generate a new version (timestamp-based for uniqueness)
  fn generate_version() -> u64 {
    // Use current timestamp as version for simplicity
    // In production, this could be a combination of timestamp and counter
    SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .map(|d| d.as_millis() as u64)
      .unwrap_or(0)
  }

  /// Serialize to bytes using postcard
  pub fn serialize(&self) -> Vec<u8> {
    postcard::to_allocvec(self).expect("serialization should succeed")
  }

  /// Deserialize from bytes using postcard
  pub fn deserialize(bytes: &[u8]) -> Result<Self, DecodeError> {
    postcard::from_bytes(bytes).map_err(|_| DecodeError::InvalidData)
  }

  /// Check if this hash has expired (given current timestamp in milliseconds)
  pub fn is_expired(&self, now_ms: u64) -> bool {
    if self.expires_at == NO_EXPIRATION {
      return false;
    }
    now_ms >= self.expires_at
  }

  /// Check if this hash has an expiration time set
  #[allow(dead_code)]
  pub fn has_expiration(&self) -> bool {
    self.expires_at != NO_EXPIRATION
  }

  /// Increment the size (when adding a new field)
  pub fn incr_size(&mut self) {
    self.size += 1;
  }

  /// Decrement the size (when removing a field)
  pub fn decr_size(&mut self) {
    if self.size > 0 {
      self.size -= 1;
    }
  }

  /// Set expiration timestamp
  #[allow(dead_code)]
  pub fn set_expiration(&mut self, expires_at: u64) {
    self.expires_at = expires_at;
  }

  /// Clear expiration (make it never expire)
  #[allow(dead_code)]
  pub fn clear_expiration(&mut self) {
    self.expires_at = NO_EXPIRATION;
  }
}

impl Default for HashMetadata {
  fn default() -> Self {
    Self::new()
  }
}

/// Errors that can occur during decoding
#[derive(Debug, Clone, PartialEq)]
pub enum DecodeError {
  /// Input data is invalid or corrupted
  InvalidData,
}

impl Display for DecodeError {
  fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
    match self {
      DecodeError::InvalidData => write!(f, "invalid data for decoding"),
    }
  }
}

impl Error for DecodeError {}

/// Hash field-value pair structure for storage
///
/// This struct represents a single field-value pair within a hash.
/// The field name is part of the RocksDB key (key|version|field),
/// and this struct is stored as the value.
///
/// # Storage Layout
///
/// ```text
///                       +---------------+
/// key|version|field =>  |     value     |
///                       +---------------+
/// ```
///
/// - `key`: the original hash key (user key)
/// - `version`: 8-byte version from HashMetadata
/// - `field`: the hash field name
/// - `value`: the hash field value (this struct)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HashFieldValue {
  /// The value of the hash field
  pub data: Vec<u8>,
}

impl HashFieldValue {
  /// Create a new HashFieldValue
  pub fn new(data: impl Into<Vec<u8>>) -> Self {
    Self { data: data.into() }
  }

  /// Serialize to bytes using postcard
  pub fn serialize(&self) -> Vec<u8> {
    postcard::to_allocvec(self).expect("serialization should succeed")
  }

  /// Deserialize from bytes using postcard
  pub fn deserialize(bytes: &[u8]) -> Result<Self, DecodeError> {
    postcard::from_bytes(bytes).map_err(|_| DecodeError::InvalidData)
  }

  /// Build the sub-key for storage: key_len|key|version|field
  ///
  /// Format:
  /// ```text
  /// +-----------+-------------+-------------+-------------+
  /// | key_len   |     key     |   version   |    field    |
  /// | (4 bytes) |  (key_len)  |  (8 bytes)  |  (variable) |
  /// +-----------+-------------+-------------+-------------+
  /// ```
  ///
  /// # Arguments
  /// * `key` - The hash key (user key)
  /// * `version` - The version from HashMetadata
  /// * `field` - The field name
  ///
  /// # Returns
  /// The composed sub-key as bytes
  pub fn build_sub_key(key: &[u8], version: u64, field: &[u8]) -> Vec<u8> {
    let key_len = key.len() as u32;
    let mut sub_key = Vec::with_capacity(4 + key.len() + 8 + field.len());
    sub_key.extend_from_slice(&key_len.to_be_bytes());
    sub_key.extend_from_slice(key);
    sub_key.extend_from_slice(&version.to_be_bytes());
    sub_key.extend_from_slice(field);
    sub_key
  }

  /// Build the sub-key as hex string for storage (guaranteed valid UTF-8)
  /// This is used for storing in String-based KV stores like rockraft
  pub fn build_sub_key_hex(key: &[u8], version: u64, field: &[u8]) -> String {
    let sub_key = Self::build_sub_key(key, version, field);
    hex::encode(&sub_key)
  }

  /// Parse a hex-encoded sub-key into its components: (key, version, field)
  #[allow(dead_code)]
  pub fn parse_sub_key_hex(hex_str: &str) -> Option<(Vec<u8>, u64, Vec<u8>)> {
    let sub_key = hex::decode(hex_str).ok()?;
    let (key, version, field) = Self::parse_sub_key(&sub_key)?;
    Some((key.to_vec(), version, field.to_vec()))
  }

  /// Parse a sub-key into its components: (key, version, field)
  ///
  /// # Arguments
  /// * `sub_key` - The composed sub-key bytes
  ///
  /// # Returns
  /// Some((key, version, field)) if parsing succeeds, None otherwise
  pub fn parse_sub_key(sub_key: &[u8]) -> Option<(&[u8], u64, &[u8])> {
    // Need at least 4 bytes for key_len + 8 bytes for version
    if sub_key.len() < 12 {
      return None;
    }

    // Parse key length (first 4 bytes)
    let key_len = u32::from_be_bytes([sub_key[0], sub_key[1], sub_key[2], sub_key[3]]) as usize;

    // Check if we have enough bytes: 4 (key_len) + key_len (key) + 8 (version) + field
    if sub_key.len() < 12 + key_len {
      return None;
    }

    // Extract components
    let key = &sub_key[4..4 + key_len];
    let version_bytes = &sub_key[4 + key_len..4 + key_len + 8];
    let version = u64::from_be_bytes([
      version_bytes[0],
      version_bytes[1],
      version_bytes[2],
      version_bytes[3],
      version_bytes[4],
      version_bytes[5],
      version_bytes[6],
      version_bytes[7],
    ]);
    let field = &sub_key[4 + key_len + 8..];

    Some((key, version, field))
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::encoding::{CURRENT_VERSION, NO_EXPIRATION};

  #[test]
  fn test_hash_metadata_new() {
    let meta = HashMetadata::new();
    assert_eq!(meta.flags, CURRENT_VERSION);
    assert_eq!(meta.expires_at, NO_EXPIRATION);
    assert_eq!(meta.size, 0);
  }

  #[test]
  fn test_hash_metadata_with_expiration() {
    let expire_time = 1893456000000;
    let meta = HashMetadata::with_expiration(expire_time);
    assert_eq!(meta.expires_at, expire_time);
    assert_eq!(meta.size, 0);
  }

  #[test]
  fn test_encode_decode_without_expiration() {
    let mut meta = HashMetadata::new();
    meta.size = 10;

    let encoded = meta.serialize();
    let decoded = HashMetadata::deserialize(&encoded).unwrap();

    assert_eq!(meta, decoded);
    assert_eq!(decoded.flags, CURRENT_VERSION);
    assert_eq!(decoded.expires_at, NO_EXPIRATION);
    assert_eq!(decoded.size, 10);
  }

  #[test]
  fn test_encode_decode_with_expiration() {
    let meta = HashMetadata::with_expiration(1234567890000);

    let encoded = meta.serialize();
    let decoded = HashMetadata::deserialize(&encoded).unwrap();

    assert_eq!(meta, decoded);
    assert_eq!(decoded.expires_at, 1234567890000);
  }

  #[test]
  fn test_is_expired() {
    let mut meta = HashMetadata::new();

    // No expiration
    assert!(!meta.is_expired(u64::MAX));
    assert!(!meta.has_expiration());

    // With expiration
    meta.expires_at = 1000;
    assert!(meta.has_expiration());
    assert!(meta.is_expired(1000));
    assert!(meta.is_expired(1001));
    assert!(!meta.is_expired(999));
  }

  #[test]
  fn test_size_operations() {
    let mut meta = HashMetadata::new();

    assert_eq!(meta.size, 0);

    meta.incr_size();
    assert_eq!(meta.size, 1);

    meta.incr_size();
    assert_eq!(meta.size, 2);

    meta.decr_size();
    assert_eq!(meta.size, 1);

    // Should not underflow
    meta.decr_size();
    assert_eq!(meta.size, 0);

    meta.decr_size();
    assert_eq!(meta.size, 0);
  }

  #[test]
  fn test_expiration_operations() {
    let mut meta = HashMetadata::new();

    assert_eq!(meta.expires_at, NO_EXPIRATION);

    meta.set_expiration(1000000);
    assert_eq!(meta.expires_at, 1000000);

    meta.clear_expiration();
    assert_eq!(meta.expires_at, NO_EXPIRATION);
  }

  #[test]
  fn test_decode_error_invalid_data() {
    // postcard can decode many inputs, so we test with truncated data
    let valid_meta = HashMetadata::new();
    let encoded = valid_meta.serialize();

    // Truncated data should fail
    if encoded.len() > 2 {
      assert_eq!(
        HashMetadata::deserialize(&encoded[..1]),
        Err(DecodeError::InvalidData)
      );
    }
  }

  // ==================== HashFieldValue Tests ====================

  #[test]
  fn test_hash_field_value_new() {
    let field_value = HashFieldValue::new(b"field_data");
    assert_eq!(field_value.data, b"field_data");
  }

  #[test]
  fn test_hash_field_value_encode_decode() {
    let field_value = HashFieldValue::new(b"hello world");
    let encoded = field_value.serialize();
    let decoded = HashFieldValue::deserialize(&encoded).unwrap();

    assert_eq!(field_value, decoded);
    assert_eq!(decoded.data, b"hello world");
  }

  #[test]
  fn test_hash_field_value_empty_data() {
    let field_value = HashFieldValue::new(b"");
    let encoded = field_value.serialize();
    let decoded = HashFieldValue::deserialize(&encoded).unwrap();

    assert_eq!(field_value, decoded);
    assert!(decoded.data.is_empty());
  }

  #[test]
  fn test_hash_field_value_large_data() {
    let data = vec![0u8; 10000];
    let field_value = HashFieldValue::new(data.clone());
    let encoded = field_value.serialize();
    let decoded = HashFieldValue::deserialize(&encoded).unwrap();

    assert_eq!(decoded.data, data);
  }

  #[test]
  fn test_build_sub_key() {
    let key = b"myhash";
    let version = 12345u64;
    let field = b"myfield";

    let sub_key = HashFieldValue::build_sub_key(key, version, field);

    // Verify structure: key_len(4) | key | version(8) | field
    let key_len = u32::from_be_bytes([sub_key[0], sub_key[1], sub_key[2], sub_key[3]]) as usize;
    assert_eq!(key_len, key.len());
    assert_eq!(&sub_key[4..4 + key_len], key);
    assert_eq!(
      &sub_key[4 + key_len..4 + key_len + 8],
      &version.to_be_bytes()
    );
    assert_eq!(&sub_key[4 + key_len + 8..], field);
  }

  #[test]
  fn test_parse_sub_key() {
    let key = b"myhash";
    let version = 12345u64;
    let field = b"myfield";

    let sub_key = HashFieldValue::build_sub_key(key, version, field);
    let (parsed_key, parsed_version, parsed_field) =
      HashFieldValue::parse_sub_key(&sub_key).unwrap();

    assert_eq!(parsed_key, key);
    assert_eq!(parsed_version, version);
    assert_eq!(parsed_field, field);
  }

  #[test]
  fn test_parse_sub_key_empty_field() {
    let key = b"myhash";
    let version = 12345u64;
    let field = b"";

    let sub_key = HashFieldValue::build_sub_key(key, version, field);
    let (parsed_key, parsed_version, parsed_field) =
      HashFieldValue::parse_sub_key(&sub_key).unwrap();

    assert_eq!(parsed_key, key);
    assert_eq!(parsed_version, version);
    assert_eq!(parsed_field, field);
  }

  #[test]
  fn test_parse_sub_key_empty_key() {
    let key = b"";
    let version = 12345u64;
    let field = b"myfield";

    let sub_key = HashFieldValue::build_sub_key(key, version, field);
    let (parsed_key, parsed_version, parsed_field) =
      HashFieldValue::parse_sub_key(&sub_key).unwrap();

    assert_eq!(parsed_key, key);
    assert_eq!(parsed_version, version);
    assert_eq!(parsed_field, field);
  }

  #[test]
  fn test_parse_sub_key_too_short() {
    // Sub-key must be at least 9 bytes (1 byte key + 8 bytes version + 0 byte field)
    let sub_key = vec![0u8; 8];
    assert!(HashFieldValue::parse_sub_key(&sub_key).is_none());

    let sub_key = vec![0u8; 7];
    assert!(HashFieldValue::parse_sub_key(&sub_key).is_none());
  }

  #[test]
  fn test_parse_sub_key_decode_error() {
    // Invalid data should fail
    assert_eq!(
      HashFieldValue::deserialize(b""),
      Err(DecodeError::InvalidData)
    );
    assert_eq!(
      HashFieldValue::deserialize(b"garbage"),
      Err(DecodeError::InvalidData)
    );
  }
}
