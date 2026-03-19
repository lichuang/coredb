//! String type encoding/decoding for storage
//!
//! String data is stored directly at the key location with metadata
//! prepended to the actual data. This is simpler than Hash/List/Set
//! types which need to store metadata and sub-keys separately.
//!
//! # Storage Layout
//!
//! ## String Value
//! ```text
//! +-----------+------------+--------------------+
//! |   flags   | expires_at |       data         |
//! | (1byte)   |  (8byte)   |     (Nbyte)        |
//! +-----------+------------+--------------------+
//! ```
//!
//! - `flags`: high 4 bits = encoding version, low 4 bits = data type
//! - `expires_at`: expiration timestamp in milliseconds, 0 means no expiration
//! - `data`: user's raw value bytes
//!
//! ## Example
//!
//! When user executes `SET key value`:
//! - The key is used as the RocksDB key
//! - The value is encoded as: `flags | expires_at | data`
//!
//! For `SET key value EX 60`:
//! - expires_at = current_timestamp + 60 * 1000
//!
//! For `SET key value` without TTL:
//! - expires_at = NO_EXPIRATION (0)

use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};

use serde::{Deserialize, Serialize};

use crate::encoding::{CURRENT_VERSION, NO_EXPIRATION};

/// String value structure for storage
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StringValue {
  /// Flags field: high 4 bits = encoding version, low 4 bits = data type
  pub flags: u8,
  /// Expiration timestamp in milliseconds (Unix timestamp), 0 means no expiration
  pub expires_at: u64,
  /// Actual data
  pub data: Vec<u8>,
}

impl StringValue {
  /// Create a new StringValue without expiration
  pub fn new(data: impl Into<Vec<u8>>) -> Self {
    Self {
      flags: CURRENT_VERSION,
      expires_at: NO_EXPIRATION,
      data: data.into(),
    }
  }

  /// Create a new StringValue with expiration timestamp (in milliseconds)
  pub fn with_expiration(data: impl Into<Vec<u8>>, expires_at: u64) -> Self {
    Self {
      flags: CURRENT_VERSION,
      expires_at,
      data: data.into(),
    }
  }

  /// Serialize to bytes using postcard
  pub fn serialize(&self) -> Vec<u8> {
    postcard::to_allocvec(self).expect("serialization should succeed")
  }

  /// Deserialize from bytes using postcard
  pub fn deserialize(bytes: &[u8]) -> Result<Self, DecodeError> {
    postcard::from_bytes(bytes).map_err(|_| DecodeError::InvalidData)
  }

  /// Check if this value has expired (given current timestamp in milliseconds)
  pub fn is_expired(&self, now_ms: u64) -> bool {
    if self.expires_at == NO_EXPIRATION {
      return false;
    }
    now_ms >= self.expires_at
  }

  /// Check if this value has an expiration time set
  pub fn has_expiration(&self) -> bool {
    self.expires_at != NO_EXPIRATION
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

#[cfg(test)]
mod tests {
  use super::*;
  use crate::encoding::{CURRENT_VERSION, NO_EXPIRATION};

  #[test]
  fn test_encode_decode_without_expiration() {
    let value = StringValue::new(b"hello world");
    let encoded = value.serialize();
    let decoded = StringValue::deserialize(&encoded).unwrap();

    assert_eq!(value, decoded);
    assert_eq!(decoded.flags, CURRENT_VERSION);
    assert_eq!(decoded.expires_at, NO_EXPIRATION);
    assert_eq!(decoded.data, b"hello world");
  }

  #[test]
  fn test_encode_decode_with_expiration() {
    let value = StringValue::with_expiration(b"hello world", 1893456000000);
    let encoded = value.serialize();
    let decoded = StringValue::deserialize(&encoded).unwrap();

    assert_eq!(value, decoded);
    assert_eq!(decoded.flags, CURRENT_VERSION);
    assert_eq!(decoded.expires_at, 1893456000000);
    assert_eq!(decoded.data, b"hello world");
  }

  #[test]
  fn test_empty_data() {
    let value = StringValue::new(b"");
    let encoded = value.serialize();
    let decoded = StringValue::deserialize(&encoded).unwrap();

    assert_eq!(value, decoded);
    assert!(decoded.data.is_empty());
  }

  #[test]
  fn test_large_data() {
    let data = vec![0u8; 10000];
    let value = StringValue::new(data.clone());
    let encoded = value.serialize();
    let decoded = StringValue::deserialize(&encoded).unwrap();

    assert_eq!(decoded.data, data);
  }

  #[test]
  fn test_is_expired() {
    let value = StringValue::with_expiration(b"data", 1000);
    assert!(value.has_expiration());
    assert!(value.is_expired(1000));
    assert!(value.is_expired(1001));
    assert!(!value.is_expired(999));

    let value_no_exp = StringValue::new(b"data");
    assert!(!value_no_exp.has_expiration());
    assert!(!value_no_exp.is_expired(u64::MAX));
  }

  #[test]
  fn test_decode_error_invalid_data() {
    // Invalid data should fail
    assert_eq!(StringValue::deserialize(b""), Err(DecodeError::InvalidData));
    assert_eq!(
      StringValue::deserialize(b"garbage"),
      Err(DecodeError::InvalidData)
    );
  }

  #[test]
  fn test_version_compatibility() {
    // Test that we can decode data with different flags values
    let value = StringValue {
      flags: 42, // Some future flags value
      expires_at: NO_EXPIRATION,
      data: b"test".to_vec(),
    };
    let encoded = value.serialize();
    let decoded = StringValue::deserialize(&encoded).unwrap();

    assert_eq!(decoded.flags, 42);
  }
}
