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
  pub fn with_expiration(expires_at: u64) -> Self {
    Self {
      flags: CURRENT_VERSION,
      expires_at: expires_at,
      version: Self::generate_version(),
      size: 0,
    }
  }

  /// Generate a new version (timestamp-based for uniqueness)
  fn generate_version() -> u64 {
    // Use current timestamp as version for simplicity
    // In production, this could be a combination of timestamp and counter
    std::time::SystemTime::now()
      .duration_since(std::time::UNIX_EPOCH)
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
  pub fn set_expiration(&mut self, expires_at: u64) {
    self.expires_at = expires_at;
  }

  /// Clear expiration (make it never expire)
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

impl std::fmt::Display for DecodeError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      DecodeError::InvalidData => write!(f, "invalid data for decoding"),
    }
  }
}

impl std::error::Error for DecodeError {}

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
}
