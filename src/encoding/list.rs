//! List type encoding/decoding for storage
//!
//! List data is stored in two parts:
//! 1. Metadata: stored at `key`, contains flags, expires_at, version, size, head, tail
//! 2. Elements: stored at `key|version|index`, each element is a separate KV pair
//!
//! This design follows KVRocks' approach with head/tail cursors that start at
//! `INITIAL_INDEX` (u64::MAX / 2) to allow O(1) push on both ends.
//!
//! # Storage Layout
//!
//! ## List Metadata
//! ```text
//! +-----------+------------+-----------+-----------+-----------+-----------+
//! |   flags   | expires_at |  version  |   size    |   head    |   tail    |
//! | (1byte)   |  (8byte)   |  (8byte)  |  (8byte)  |  (8byte)  |  (8byte)  |
//! +-----------+------------+-----------+-----------+-----------+-----------+
//! ```
//!
//! - `flags`: high 4 bits = encoding version, low 4 bits = data type (0x03 for list)
//! - `expires_at`: expiration timestamp in milliseconds, 0 means no expiration
//! - `version`: used for fast deletion (increment to invalidate all sub-keys)
//! - `size`: number of elements in this list
//! - `head`: index of the leftmost element (decremented by LPUSH)
//! - `tail`: index one past the rightmost element (incremented by RPUSH)
//!
//! ## List Element
//! ```text
//!                          +---------------+
//! key|version|index     => |     data      |
//!                          +---------------+
//! ```
//!
//! # Example
//!
//! After `RPUSH mylist a b c`:
//! ```text
//! Metadata:  {flags:0x13, expires_at:0, version:V, size:3, head:INIT, tail:INIT+3}
//!
//! Sub-keys:
//!   key|V|INIT   => "a"
//!   key|V|INIT+1 => "b"
//!   key|V|INIT+2 => "c"
//! ```
//!
//! After `LPUSH mylist x`:
//! ```text
//! Metadata:  {flags:0x13, expires_at:0, version:V, size:4, head:INIT-1, tail:INIT+3}
//!
//! Sub-keys:
//!   key|V|INIT-1 => "x"      <- new element (leftmost)
//!   key|V|INIT   => "a"
//!   key|V|INIT+1 => "b"
//!   key|V|INIT+2 => "c"
//! ```

use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::encoding::{CURRENT_VERSION, NO_EXPIRATION, TYPE_LIST};

/// Initial index for head and tail cursors.
///
/// We start at the midpoint of the u64 range so both LPush (decrementing)
/// and RPush (incrementing) have ample room before overflow.
pub const INITIAL_INDEX: u64 = u64::MAX / 2;

/// List metadata structure for storage
///
/// Stored at the user key in RocksDB. Tracks the list boundaries (head/tail)
/// and element count.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ListMetadata {
  /// Flags field: high 4 bits = encoding version, low 4 bits = data type
  pub flags: u8,
  /// Expiration timestamp in milliseconds (Unix timestamp), 0 means no expiration
  pub expires_at: u64,
  /// Version for fast deletion (incremented on each recreation)
  pub version: u64,
  /// Number of elements in this list
  pub size: u64,
  /// Index of the leftmost element (head of the list)
  pub head: u64,
  /// Index one past the rightmost element (tail of the list)
  pub tail: u64,
}

#[allow(dead_code)]
impl ListMetadata {
  /// Create a new empty ListMetadata without expiration
  pub fn new() -> Self {
    Self {
      flags: (CURRENT_VERSION << 4) | TYPE_LIST,
      expires_at: NO_EXPIRATION,
      version: Self::generate_version(),
      size: 0,
      head: INITIAL_INDEX,
      tail: INITIAL_INDEX,
    }
  }

  /// Create a new ListMetadata with expiration timestamp (in milliseconds)
  #[allow(dead_code)]
  pub fn with_expiration(expires_at: u64) -> Self {
    Self {
      flags: (CURRENT_VERSION << 4) | TYPE_LIST,
      expires_at,
      version: Self::generate_version(),
      size: 0,
      head: INITIAL_INDEX,
      tail: INITIAL_INDEX,
    }
  }

  /// Generate a new version (timestamp-based for uniqueness)
  fn generate_version() -> u64 {
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

  /// Check if this list has expired (given current timestamp in milliseconds)
  pub fn is_expired(&self, now_ms: u64) -> bool {
    if self.expires_at == NO_EXPIRATION {
      return false;
    }
    now_ms >= self.expires_at
  }

  /// Check if this list has an expiration time set
  #[allow(dead_code)]
  pub fn has_expiration(&self) -> bool {
    self.expires_at != NO_EXPIRATION
  }

  /// Get the type from flags (low 4 bits)
  pub fn get_type(&self) -> u8 {
    self.flags & 0x0F
  }

  /// Get the physical index for the i-th element from the left (0-based)
  ///
  /// Returns `None` if `index` is out of range.
  pub fn index_at(&self, index: u64) -> Option<u64> {
    if index >= self.size {
      return None;
    }
    Some(self.head + index)
  }

  /// Resolve a Redis-style index (supports negative indices).
  ///
  /// - Positive: 0-based from left
  /// - Negative: -1 is last element, -2 is second to last, etc.
  ///
  /// Returns `None` if the resolved index is out of range.
  pub fn resolve_index(&self, index: i64) -> Option<u64> {
    let physical = if index < 0 {
      // -1 maps to size-1, -2 maps to size-2, etc.
      let from_end = index.unsigned_abs();
      if from_end > self.size {
        return None;
      }
      self.size - from_end
    } else {
      index as u64
    };

    if physical >= self.size {
      return None;
    }

    Some(self.head + physical)
  }
}

impl Default for ListMetadata {
  fn default() -> Self {
    Self::new()
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

/// List element value structure for storage
///
/// Each list element is stored as a separate KV pair. The element value
/// is stored as the RocksDB value, and the key encodes the list key,
/// version, and element index.
///
/// # Storage Layout
///
/// ```text
///                          +---------------+
/// key|version|index     => |     data      |
///                          +---------------+
/// ```
///
/// - `key`: the original list key (user key)
/// - `version`: 8-byte version from ListMetadata
/// - `index`: 8-byte element index (u64, big-endian)
/// - `data`: the element value (this struct)
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ListElementValue {
  /// The element value
  pub data: Vec<u8>,
}

#[allow(dead_code)]
impl ListElementValue {
  #[allow(dead_code)]
  /// Create a new ListElementValue
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

  /// Build the sub-key for storage: key_len|key|version|index
  ///
  /// Format:
  /// ```text
  /// +-----------+-------------+-------------+-------------+
  /// | key_len   |     key     |   version   |    index   |
  /// | (4 bytes) |  (key_len)  |  (8 bytes)  |  (8 bytes) |
  /// +-----------+-------------+-------------+-------------+
  /// ```
  ///
  /// # Arguments
  /// * `key` - The list key (user key)
  /// * `version` - The version from ListMetadata
  /// * `index` - The element index (physical index from metadata)
  ///
  /// # Returns
  /// The composed sub-key as bytes
  pub fn build_sub_key(key: &[u8], version: u64, index: u64) -> Vec<u8> {
    let key_len = key.len() as u32;
    let mut sub_key = Vec::with_capacity(4 + key.len() + 8 + 8);
    sub_key.extend_from_slice(&key_len.to_be_bytes());
    sub_key.extend_from_slice(key);
    sub_key.extend_from_slice(&version.to_be_bytes());
    sub_key.extend_from_slice(&index.to_be_bytes());
    sub_key
  }

  /// Build the sub-key as hex string for storage (guaranteed valid UTF-8)
  /// This is used for storing in String-based KV stores like rockraft
  pub fn build_sub_key_hex(key: &[u8], version: u64, index: u64) -> String {
    let sub_key = Self::build_sub_key(key, version, index);
    hex::encode(&sub_key)
  }

  /// Build the hex-encoded prefix for scanning all elements of a list
  ///
  /// Format: hex(key_len(4 bytes) | key | version(8 bytes))
  pub fn build_prefix_hex(key: &[u8], version: u64) -> String {
    let key_len = key.len() as u32;
    let mut prefix = Vec::with_capacity(4 + key.len() + 8);
    prefix.extend_from_slice(&key_len.to_be_bytes());
    prefix.extend_from_slice(key);
    prefix.extend_from_slice(&version.to_be_bytes());
    hex::encode(&prefix)
  }

  /// Parse a hex-encoded sub-key into its components: (key, version, index)
  #[allow(dead_code)]
  pub fn parse_sub_key_hex(hex_str: &str) -> Option<(Vec<u8>, u64, u64)> {
    let sub_key = hex::decode(hex_str).ok()?;
    let (key, version, index) = Self::parse_sub_key(&sub_key)?;
    Some((key.to_vec(), version, index))
  }

  /// Parse a sub-key into its components: (key, version, index)
  ///
  /// # Arguments
  /// * `sub_key` - The composed sub-key bytes
  ///
  /// # Returns
  /// Some((key, version, index)) if parsing succeeds, None otherwise
  pub fn parse_sub_key(sub_key: &[u8]) -> Option<(&[u8], u64, u64)> {
    // Need at least 4 bytes for key_len + 8 bytes for version + 8 bytes for index
    if sub_key.len() < 20 {
      return None;
    }

    // Parse key length (first 4 bytes)
    let key_len = u32::from_be_bytes([sub_key[0], sub_key[1], sub_key[2], sub_key[3]]) as usize;

    // Check if we have enough bytes
    if sub_key.len() < 20 + key_len {
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
    let index_bytes = &sub_key[4 + key_len + 8..4 + key_len + 16];
    let index = u64::from_be_bytes([
      index_bytes[0],
      index_bytes[1],
      index_bytes[2],
      index_bytes[3],
      index_bytes[4],
      index_bytes[5],
      index_bytes[6],
      index_bytes[7],
    ]);

    Some((key, version, index))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  // ==================== ListMetadata Tests ====================

  #[test]
  fn test_list_metadata_new() {
    let meta = ListMetadata::new();
    assert_eq!(meta.flags, (CURRENT_VERSION << 4) | TYPE_LIST);
    assert_eq!(meta.expires_at, NO_EXPIRATION);
    assert_eq!(meta.size, 0);
    assert_eq!(meta.head, INITIAL_INDEX);
    assert_eq!(meta.tail, INITIAL_INDEX);
  }

  #[test]
  fn test_list_metadata_encode_decode() {
    let meta = ListMetadata::new();
    let encoded = meta.serialize();
    let decoded = ListMetadata::deserialize(&encoded).unwrap();

    assert_eq!(meta, decoded);
    assert_eq!(decoded.flags, (CURRENT_VERSION << 4) | TYPE_LIST);
    assert_eq!(decoded.expires_at, NO_EXPIRATION);
    assert_eq!(decoded.size, 0);
    assert_eq!(decoded.head, INITIAL_INDEX);
    assert_eq!(decoded.tail, INITIAL_INDEX);
  }

  #[test]
  fn test_list_metadata_with_expiration() {
    let meta = ListMetadata::with_expiration(1893456000000);
    let encoded = meta.serialize();
    let decoded = ListMetadata::deserialize(&encoded).unwrap();

    assert_eq!(meta, decoded);
    assert_eq!(decoded.expires_at, 1893456000000);
    assert_eq!(decoded.size, 0);
    assert_eq!(decoded.head, INITIAL_INDEX);
    assert_eq!(decoded.tail, INITIAL_INDEX);
  }

  #[test]
  fn test_list_metadata_with_size_and_indices() {
    let mut meta = ListMetadata::new();
    meta.size = 3;
    meta.head = INITIAL_INDEX - 1;
    meta.tail = INITIAL_INDEX + 2;

    let encoded = meta.serialize();
    let decoded = ListMetadata::deserialize(&encoded).unwrap();

    assert_eq!(meta, decoded);
    assert_eq!(decoded.size, 3);
    assert_eq!(decoded.head, INITIAL_INDEX - 1);
    assert_eq!(decoded.tail, INITIAL_INDEX + 2);
  }

  #[test]
  fn test_list_metadata_is_expired() {
    let mut meta = ListMetadata::new();

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
  fn test_list_metadata_get_type() {
    let meta = ListMetadata::new();
    assert_eq!(meta.get_type(), TYPE_LIST);
  }

  #[test]
  fn test_list_metadata_index_at() {
    let mut meta = ListMetadata::new();
    meta.size = 5;
    meta.head = INITIAL_INDEX;

    assert_eq!(meta.index_at(0), Some(INITIAL_INDEX));
    assert_eq!(meta.index_at(2), Some(INITIAL_INDEX + 2));
    assert_eq!(meta.index_at(4), Some(INITIAL_INDEX + 4));
    assert_eq!(meta.index_at(5), None); // out of range
    assert_eq!(meta.index_at(100), None);
  }

  #[test]
  fn test_list_metadata_resolve_index_positive() {
    let mut meta = ListMetadata::new();
    meta.size = 5;
    meta.head = INITIAL_INDEX;

    assert_eq!(meta.resolve_index(0), Some(INITIAL_INDEX));
    assert_eq!(meta.resolve_index(2), Some(INITIAL_INDEX + 2));
    assert_eq!(meta.resolve_index(4), Some(INITIAL_INDEX + 4));
    assert_eq!(meta.resolve_index(5), None); // out of range
  }

  #[test]
  fn test_list_metadata_resolve_index_negative() {
    let mut meta = ListMetadata::new();
    meta.size = 5;
    meta.head = INITIAL_INDEX;

    // -1 = last element (index 4)
    assert_eq!(meta.resolve_index(-1), Some(INITIAL_INDEX + 4));
    // -2 = second to last (index 3)
    assert_eq!(meta.resolve_index(-2), Some(INITIAL_INDEX + 3));
    // -5 = first element (index 0)
    assert_eq!(meta.resolve_index(-5), Some(INITIAL_INDEX));
    // -6 = out of range
    assert_eq!(meta.resolve_index(-6), None);
  }

  #[test]
  fn test_list_metadata_resolve_index_empty_list() {
    let meta = ListMetadata::new();
    assert_eq!(meta.resolve_index(0), None);
    assert_eq!(meta.resolve_index(-1), None);
  }

  #[test]
  fn test_decode_error_invalid_data() {
    let valid_meta = ListMetadata::new();
    let encoded = valid_meta.serialize();

    if encoded.len() > 2 {
      assert_eq!(
        ListMetadata::deserialize(&encoded[..1]),
        Err(DecodeError::InvalidData)
      );
    }
  }

  // ==================== ListElementValue Tests ====================

  #[test]
  fn test_list_element_value_new() {
    let elem = ListElementValue::new(b"hello");
    assert_eq!(elem.data, b"hello");
  }

  #[test]
  fn test_list_element_value_encode_decode() {
    let elem = ListElementValue::new(b"world");
    let encoded = elem.serialize();
    let decoded = ListElementValue::deserialize(&encoded).unwrap();

    assert_eq!(elem, decoded);
    assert_eq!(decoded.data, b"world");
  }

  #[test]
  fn test_list_element_value_empty() {
    let elem = ListElementValue::new(b"");
    let encoded = elem.serialize();
    let decoded = ListElementValue::deserialize(&encoded).unwrap();

    assert_eq!(elem, decoded);
    assert!(decoded.data.is_empty());
  }

  #[test]
  fn test_list_element_value_large() {
    let data = vec![0u8; 10000];
    let elem = ListElementValue::new(data.clone());
    let encoded = elem.serialize();
    let decoded = ListElementValue::deserialize(&encoded).unwrap();

    assert_eq!(decoded.data, data);
  }

  // ==================== Sub-key Tests ====================

  #[test]
  fn test_build_sub_key() {
    let key = b"mylist";
    let version = 12345u64;
    let index = INITIAL_INDEX;

    let sub_key = ListElementValue::build_sub_key(key, version, index);

    // Verify structure: key_len(4) | key | version(8) | index(8)
    assert_eq!(sub_key.len(), 4 + 6 + 8 + 8);

    let key_len = u32::from_be_bytes([sub_key[0], sub_key[1], sub_key[2], sub_key[3]]) as usize;
    assert_eq!(key_len, 6);
    assert_eq!(&sub_key[4..10], b"mylist");

    let version_bytes = &sub_key[10..18];
    assert_eq!(version_bytes, &version.to_be_bytes());

    let index_bytes = &sub_key[18..26];
    assert_eq!(index_bytes, &index.to_be_bytes());
  }

  #[test]
  fn test_parse_sub_key() {
    let key = b"mylist";
    let version = 12345u64;
    let index = INITIAL_INDEX + 5;

    let sub_key = ListElementValue::build_sub_key(key, version, index);
    let (parsed_key, parsed_version, parsed_index) =
      ListElementValue::parse_sub_key(&sub_key).unwrap();

    assert_eq!(parsed_key, key);
    assert_eq!(parsed_version, version);
    assert_eq!(parsed_index, index);
  }

  #[test]
  fn test_parse_sub_key_empty_key() {
    let key = b"";
    let version = 999u64;
    let index = INITIAL_INDEX;

    let sub_key = ListElementValue::build_sub_key(key, version, index);
    let (parsed_key, parsed_version, parsed_index) =
      ListElementValue::parse_sub_key(&sub_key).unwrap();

    assert_eq!(parsed_key, b"");
    assert_eq!(parsed_version, version);
    assert_eq!(parsed_index, index);
  }

  #[test]
  fn test_parse_sub_key_too_short() {
    // Must be at least 20 bytes
    let sub_key = vec![0u8; 19];
    assert!(ListElementValue::parse_sub_key(&sub_key).is_none());

    let sub_key = vec![0u8; 10];
    assert!(ListElementValue::parse_sub_key(&sub_key).is_none());
  }

  #[test]
  fn test_parse_sub_key_truncated_key() {
    // key_len says 100 but we only have 20 bytes total
    let mut sub_key = Vec::new();
    sub_key.extend_from_slice(&100u32.to_be_bytes()); // key_len = 100
    sub_key.extend_from_slice(b"short"); // only 5 bytes of key
    // missing version and index

    assert!(ListElementValue::parse_sub_key(&sub_key).is_none());
  }

  #[test]
  fn test_build_sub_key_hex_roundtrip() {
    let key = b"mylist";
    let version = 12345u64;
    let index = INITIAL_INDEX - 10;

    let hex_str = ListElementValue::build_sub_key_hex(key, version, index);
    let (parsed_key, parsed_version, parsed_index) =
      ListElementValue::parse_sub_key_hex(&hex_str).unwrap();

    assert_eq!(parsed_key, key.to_vec());
    assert_eq!(parsed_version, version);
    assert_eq!(parsed_index, index);
  }

  #[test]
  fn test_build_prefix_hex() {
    let key = b"mylist";
    let version = 12345u64;

    let prefix_hex = ListElementValue::build_prefix_hex(key, version);

    // Decode and verify: key_len(4) | key | version(8)
    let prefix_bytes = hex::decode(&prefix_hex).unwrap();
    assert_eq!(prefix_bytes.len(), 4 + 6 + 8);

    let key_len = u32::from_be_bytes([
      prefix_bytes[0],
      prefix_bytes[1],
      prefix_bytes[2],
      prefix_bytes[3],
    ]);
    assert_eq!(key_len as usize, 6);
    assert_eq!(&prefix_bytes[4..10], b"mylist");

    let version_bytes = &prefix_bytes[10..18];
    assert_eq!(version_bytes, &version.to_be_bytes());
  }

  #[test]
  fn test_build_prefix_hex_is_valid_utf8() {
    let key = b"test";
    let version = 42u64;

    let prefix_hex = ListElementValue::build_prefix_hex(key, version);
    assert!(prefix_hex.is_ascii());
    assert!(String::from_utf8(prefix_hex.into_bytes()).is_ok());
  }

  #[test]
  fn test_sub_key_ordering() {
    // Verify that sub-keys with increasing indices produce lexicographically
    // increasing hex strings (important for range scans)
    let key = b"mylist";
    let version = 100u64;

    let sk0 = ListElementValue::build_sub_key_hex(key, version, INITIAL_INDEX);
    let sk1 = ListElementValue::build_sub_key_hex(key, version, INITIAL_INDEX + 1);
    let sk2 = ListElementValue::build_sub_key_hex(key, version, INITIAL_INDEX - 1);

    // Since INITIAL_INDEX uses big-endian encoding, larger indices produce
    // lexicographically larger hex strings
    assert!(sk0 < sk1);
    // Smaller index should produce lexicographically smaller string
    assert!(sk2 < sk0);
  }
}
