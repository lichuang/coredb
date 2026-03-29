//! JSON type encoding/decoding for storage
//!
//! JSON data is stored similarly to String, with an additional `format` field
//! to distinguish the encoding format. Currently only JSON format (0) is supported.
//!
//! This design follows KVRocks' approach where JSON is stored as a single
//! value (no sub-keys), making it simple and efficient for IO-friendly access.
//!
//! # Storage Layout
//!
//! ```text
//! +----------+------------+-----------+--------------------+
//! |   flags   | expires_at |  format   |       payload      |
//! | (1byte)  |  (8byte)  |  (1byte)  |       (Nbyte)      |
//! +----------+------------+-----------+--------------------+
//! ```
//!
//! - `flags`: high 4 bits = encoding version, low 4 bits = data type (0x0A)
//! - `expires_at`: expiration timestamp in milliseconds, 0 means no expiration
//! - `format`: 0 = JSON, 1 = CBOR (reserved for future use)
//! - `payload`: the raw JSON string bytes
//!
//! # Example
//!
//! After `JSON.SET mykey $ '{"name":"alice","age":30}'`:
//! ```text
//! key => {flags:0x1A, expires_at:0, format:0, payload:'{"name":"alice","age":30}'}
//! ```

use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};

use serde::{Deserialize, Serialize};

use crate::encoding::{CURRENT_VERSION, NO_EXPIRATION, TYPE_JSON};

/// JSON format constants
pub const JSON_FORMAT_JSON: u8 = 0;

#[allow(dead_code)]
pub const JSON_FORMAT_CBOR: u8 = 1;

/// JSON value structure for storage
///
/// Stored at the user key in RocksDB as a single value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonMetadata {
  /// Flags field: high 4 bits = encoding version, low 4 bits = data type
  pub flags: u8,
  /// Expiration timestamp in milliseconds (Unix timestamp), 0 means no expiration
  pub expires_at: u64,
  /// Format of the JSON payload (0 = JSON, 1 = CBOR)
  pub format: u8,
  /// The raw JSON payload bytes
  pub payload: Vec<u8>,
}

#[allow(dead_code)]
impl JsonMetadata {
  /// Create a new JsonMetadata with JSON format, without expiration
  pub fn new(data: impl Into<Vec<u8>>) -> Self {
    Self {
      flags: (CURRENT_VERSION << 4) | TYPE_JSON,
      expires_at: NO_EXPIRATION,
      format: JSON_FORMAT_JSON,
      payload: data.into(),
    }
  }

  /// Create a new JsonMetadata with expiration timestamp (in milliseconds)
  pub fn with_expiration(data: impl Into<Vec<u8>>, expires_at: u64) -> Self {
    Self {
      flags: (CURRENT_VERSION << 4) | TYPE_JSON,
      expires_at,
      format: JSON_FORMAT_JSON,
      payload: data.into(),
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

  /// Check if this JSON value has expired
  pub fn is_expired(&self, now_ms: u64) -> bool {
    if self.expires_at == NO_EXPIRATION {
      return false;
    }
    now_ms >= self.expires_at
  }

  /// Check if this JSON value has an expiration time set
  pub fn has_expiration(&self) -> bool {
    self.expires_at != NO_EXPIRATION
  }

  /// Set expiration timestamp
  pub fn set_expiration(&mut self, expires_at: u64) {
    self.expires_at = expires_at;
  }

  /// Clear expiration
  pub fn clear_expiration(&mut self) {
    self.expires_at = NO_EXPIRATION;
  }

  /// Get the type from flags (low 4 bits)
  pub fn get_type(&self) -> u8 {
    self.flags & 0x0F
  }
}

/// Errors that can occur during decoding
#[allow(dead_code)]
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
  use crate::encoding::{NO_EXPIRATION, TYPE_JSON};

  #[test]
  fn test_json_metadata_new() {
    let meta = JsonMetadata::new(r#"{"a":1}"#);
    assert_eq!(meta.flags, (CURRENT_VERSION << 4) | TYPE_JSON);
    assert_eq!(meta.expires_at, NO_EXPIRATION);
    assert_eq!(meta.format, JSON_FORMAT_JSON);
    assert_eq!(meta.payload, br#"{"a":1}"#);
  }

  #[test]
  fn test_json_metadata_encode_decode() {
    let meta = JsonMetadata::new(r#"{"name":"alice","age":30}"#);
    let encoded = meta.serialize();
    let decoded = JsonMetadata::deserialize(&encoded).unwrap();
    assert_eq!(meta, decoded);
  }

  #[test]
  fn test_json_metadata_with_expiration() {
    let meta = JsonMetadata::with_expiration(r#"{"a":1}"#, 1893456000000);
    let encoded = meta.serialize();
    let decoded = JsonMetadata::deserialize(&encoded).unwrap();
    assert_eq!(meta, decoded);
    assert_eq!(decoded.expires_at, 1893456000000);
  }

  #[test]
  fn test_json_metadata_is_expired() {
    let mut meta = JsonMetadata::new(r#"{"a":1}"#);
    assert!(!meta.is_expired(u64::MAX));
    assert!(!meta.has_expiration());

    meta.expires_at = 1000;
    assert!(meta.has_expiration());
    assert!(meta.is_expired(1000));
    assert!(meta.is_expired(1001));
    assert!(!meta.is_expired(999));
  }

  #[test]
  fn test_json_metadata_expiration_ops() {
    let mut meta = JsonMetadata::new(r#"{"a":1}"#);
    assert_eq!(meta.expires_at, NO_EXPIRATION);

    meta.set_expiration(1000000);
    assert_eq!(meta.expires_at, 1000000);

    meta.clear_expiration();
    assert_eq!(meta.expires_at, NO_EXPIRATION);
  }

  #[test]
  fn test_json_metadata_get_type() {
    let meta = JsonMetadata::new(r#"{"a":1}"#);
    assert_eq!(meta.get_type(), TYPE_JSON);
  }

  #[test]
  fn test_json_metadata_empty_payload() {
    let meta = JsonMetadata::new("");
    let encoded = meta.serialize();
    let decoded = JsonMetadata::deserialize(&encoded).unwrap();
    assert_eq!(meta, decoded);
    assert!(decoded.payload.is_empty());
  }

  #[test]
  fn test_json_metadata_large_payload() {
    let data = vec![b'x'; 10000];
    let meta = JsonMetadata::new(data.clone());
    let encoded = meta.serialize();
    let decoded = JsonMetadata::deserialize(&encoded).unwrap();
    assert_eq!(decoded.payload, data);
  }

  #[test]
  fn test_json_metadata_nested_json() {
    let json_str = r#"{"users":[{"id":1,"name":"alice"},{"id":2,"name":"bob"}],"count":2}"#;
    let meta = JsonMetadata::new(json_str);
    let encoded = meta.serialize();
    let decoded = JsonMetadata::deserialize(&encoded).unwrap();
    assert_eq!(meta, decoded);
    assert_eq!(String::from_utf8_lossy(&decoded.payload), json_str);
  }

  #[test]
  fn test_json_metadata_binary_payload() {
    let data: Vec<u8> = vec![0, 1, 255, 128, 0, 1, 2, 3];
    let meta = JsonMetadata::new(data.clone());
    let encoded = meta.serialize();
    let decoded = JsonMetadata::deserialize(&encoded).unwrap();
    assert_eq!(decoded.payload, data);
  }

  #[test]
  fn test_json_metadata_with_bytes() {
    let meta = JsonMetadata::new(b"hello");
    let encoded = meta.serialize();
    let decoded = JsonMetadata::deserialize(&encoded).unwrap();
    assert_eq!(decoded.payload, b"hello");
  }

  #[test]
  fn test_decode_error() {
    let valid = JsonMetadata::new(r#"{"a":1}"#);
    let encoded = valid.serialize();
    if encoded.len() > 2 {
      assert_eq!(
        JsonMetadata::deserialize(&encoded[..1]),
        Err(DecodeError::InvalidData)
      );
    }
  }

  #[test]
  fn test_json_format_constants() {
    assert_eq!(JSON_FORMAT_JSON, 0);
    assert_eq!(JSON_FORMAT_CBOR, 1);
  }
}
