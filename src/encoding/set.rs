//! Set type encoding/decoding for storage
//!
//! Set data is stored in two parts:
//! 1. Metadata: stored at `key`, contains flags, expires_at, version, size
//! 2. Members: stored at `key|version|member`, value is always empty (existence = membership)
//!
//! This design follows KVRocks' approach where a set is essentially a hash
//! with the value of sub-key always being null.
//!
//! # Storage Layout
//!
//! ## Set Metadata
//! ```text
//! +-----------+------------+-----------+-----------+
//! |   flags   | expires_at |  version  |   size    |
//! | (1byte)   |  (8byte)   |  (8byte)  |  (8byte)  |
//! +-----------+------------+-----------+-----------+
//! ```
//!
//! - `flags`: high 4 bits = encoding version, low 4 bits = data type (0x04 for set)
//! - `expires_at`: expiration timestamp in milliseconds, 0 means no expiration
//! - `version`: used for fast deletion (increment to invalidate all sub-keys)
//! - `size`: number of members in this set
//!
//! ## Set Member
//! ```text
//!                         +---------------+
//! key|version|member   => |     NULL      |
//!                         +---------------+
//! ```
//!
//! # Example
//!
//! After `SADD myset a b c`:
//! ```text
//! Metadata:  {flags:0x14, expires_at:0, version:V, size:3}
//!
//! Sub-keys:
//!   key|V|a => ""
//!   key|V|b => ""
//!   key|V|c => ""
//! ```

use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::encoding::{CURRENT_VERSION, NO_EXPIRATION, TYPE_SET};

/// Set metadata structure for storage
///
/// Stored at the user key in RocksDB. Tracks the member count.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SetMetadata {
  /// Flags field: high 4 bits = encoding version, low 4 bits = data type
  pub flags: u8,
  /// Expiration timestamp in milliseconds (Unix timestamp), 0 means no expiration
  pub expires_at: u64,
  /// Version for fast deletion (incremented on each recreation)
  pub version: u64,
  /// Number of members in this set
  pub size: u64,
}

impl SetMetadata {
  /// Create a new empty SetMetadata without expiration
  pub fn new() -> Self {
    Self {
      flags: (CURRENT_VERSION << 4) | TYPE_SET,
      expires_at: NO_EXPIRATION,
      version: Self::generate_version(),
      size: 0,
    }
  }

  /// Create a new SetMetadata with expiration timestamp (in milliseconds)
  #[allow(dead_code)]
  pub fn with_expiration(expires_at: u64) -> Self {
    Self {
      flags: (CURRENT_VERSION << 4) | TYPE_SET,
      expires_at,
      version: Self::generate_version(),
      size: 0,
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

  /// Check if this set has expired (given current timestamp in milliseconds)
  pub fn is_expired(&self, now_ms: u64) -> bool {
    if self.expires_at == NO_EXPIRATION {
      return false;
    }
    now_ms >= self.expires_at
  }

  /// Check if this set has an expiration time set
  #[allow(dead_code)]
  pub fn has_expiration(&self) -> bool {
    self.expires_at != NO_EXPIRATION
  }

  /// Increment the size (when adding a new member)
  pub fn incr_size(&mut self) {
    self.size += 1;
  }

  /// Decrement the size (when removing a member)
  #[allow(dead_code)]
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

  /// Get the type from flags (low 4 bits)
  pub fn get_type(&self) -> u8 {
    self.flags & 0x0F
  }
}

impl Default for SetMetadata {
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

/// Set member sub-key builder/parser
///
/// Each set member is stored as a separate KV pair where the value is always
/// empty. The member name is encoded in the RocksDB key alongside the set key
/// and version. The existence of the key itself proves membership.
///
/// # Storage Layout
///
/// ```text
///                          +---------------+
/// key|version|member    => |     ""       |
///                          +---------------+
/// ```
///
/// - `key`: the original set key (user key)
/// - `version`: 8-byte version from SetMetadata
/// - `member`: the member value
/// - `value`: always empty (membership is determined by key existence)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SetMemberValue;

impl SetMemberValue {
  /// Serialize to bytes (empty value for set members)
  pub fn serialize(&self) -> Vec<u8> {
    Vec::new()
  }

  /// Deserialize from bytes (empty value expected for set members)
  #[allow(dead_code)]
  pub fn deserialize(bytes: &[u8]) -> Result<Self, DecodeError> {
    if bytes.is_empty() {
      Ok(Self)
    } else {
      Err(DecodeError::InvalidData)
    }
  }

  /// Build the sub-key for storage: key_len|key|version|member
  ///
  /// Format:
  /// ```text
  /// +-----------+-------------+-------------+-------------+
  /// | key_len   |     key     |   version   |   member   |
  /// | (4 bytes) |  (key_len)  |  (8 bytes)  | (variable) |
  /// +-----------+-------------+-------------+-------------+
  /// ```
  ///
  /// # Arguments
  /// * `key` - The set key (user key)
  /// * `version` - The version from SetMetadata
  /// * `member` - The member value
  ///
  /// # Returns
  /// The composed sub-key as bytes
  pub fn build_sub_key(key: &[u8], version: u64, member: &[u8]) -> Vec<u8> {
    let key_len = key.len() as u32;
    let mut sub_key = Vec::with_capacity(4 + key.len() + 8 + member.len());
    sub_key.extend_from_slice(&key_len.to_be_bytes());
    sub_key.extend_from_slice(key);
    sub_key.extend_from_slice(&version.to_be_bytes());
    sub_key.extend_from_slice(member);
    sub_key
  }

  /// Build the sub-key as hex string for storage (guaranteed valid UTF-8)
  /// This is used for storing in String-based KV stores like rockraft
  pub fn build_sub_key_hex(key: &[u8], version: u64, member: &[u8]) -> String {
    let sub_key = Self::build_sub_key(key, version, member);
    hex::encode(&sub_key)
  }

  /// Build the hex-encoded prefix for scanning all members of a set
  ///
  /// Format: hex(key_len(4 bytes) | key | version(8 bytes))
  #[allow(dead_code)]
  pub fn build_prefix_hex(key: &[u8], version: u64) -> String {
    let key_len = key.len() as u32;
    let mut prefix = Vec::with_capacity(4 + key.len() + 8);
    prefix.extend_from_slice(&key_len.to_be_bytes());
    prefix.extend_from_slice(key);
    prefix.extend_from_slice(&version.to_be_bytes());
    hex::encode(&prefix)
  }

  /// Parse a hex-encoded sub-key into its components: (key, version, member)
  #[allow(dead_code)]
  pub fn parse_sub_key_hex(hex_str: &str) -> Option<(Vec<u8>, u64, Vec<u8>)> {
    let sub_key = hex::decode(hex_str).ok()?;
    let (key, version, member) = Self::parse_sub_key(&sub_key)?;
    Some((key.to_vec(), version, member.to_vec()))
  }

  /// Parse a sub-key into its components: (key, version, member)
  ///
  /// # Arguments
  /// * `sub_key` - The composed sub-key bytes
  ///
  /// # Returns
  /// Some((key, version, member)) if parsing succeeds, None otherwise
  pub fn parse_sub_key(sub_key: &[u8]) -> Option<(&[u8], u64, &[u8])> {
    // Need at least 4 bytes for key_len + 8 bytes for version
    if sub_key.len() < 12 {
      return None;
    }

    // Parse key length (first 4 bytes)
    let key_len = u32::from_be_bytes([sub_key[0], sub_key[1], sub_key[2], sub_key[3]]) as usize;

    // Check if we have enough bytes: 4 (key_len) + key_len (key) + 8 (version) + member
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
    let member = &sub_key[4 + key_len + 8..];

    Some((key, version, member))
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::encoding::{NO_EXPIRATION, TYPE_SET};

  // ==================== SetMetadata Tests ====================

  #[test]
  fn test_set_metadata_new() {
    let meta = SetMetadata::new();
    assert_eq!(meta.flags, (CURRENT_VERSION << 4) | TYPE_SET);
    assert_eq!(meta.expires_at, NO_EXPIRATION);
    assert_eq!(meta.size, 0);
  }

  #[test]
  fn test_set_metadata_encode_decode() {
    let meta = SetMetadata::new();
    let encoded = meta.serialize();
    let decoded = SetMetadata::deserialize(&encoded).unwrap();

    assert_eq!(meta, decoded);
    assert_eq!(decoded.flags, (CURRENT_VERSION << 4) | TYPE_SET);
    assert_eq!(decoded.expires_at, NO_EXPIRATION);
    assert_eq!(decoded.size, 0);
  }

  #[test]
  fn test_set_metadata_with_expiration() {
    let meta = SetMetadata::with_expiration(1893456000000);
    let encoded = meta.serialize();
    let decoded = SetMetadata::deserialize(&encoded).unwrap();

    assert_eq!(meta, decoded);
    assert_eq!(decoded.expires_at, 1893456000000);
    assert_eq!(decoded.size, 0);
  }

  #[test]
  fn test_set_metadata_with_size() {
    let mut meta = SetMetadata::new();
    meta.size = 10;

    let encoded = meta.serialize();
    let decoded = SetMetadata::deserialize(&encoded).unwrap();

    assert_eq!(meta, decoded);
    assert_eq!(decoded.flags, (CURRENT_VERSION << 4) | TYPE_SET);
    assert_eq!(decoded.size, 10);
  }

  #[test]
  fn test_set_metadata_is_expired() {
    let mut meta = SetMetadata::new();

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
  fn test_set_metadata_get_type() {
    let meta = SetMetadata::new();
    assert_eq!(meta.get_type(), TYPE_SET);
  }

  #[test]
  fn test_set_metadata_size_operations() {
    let mut meta = SetMetadata::new();

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
  fn test_set_metadata_expiration_operations() {
    let mut meta = SetMetadata::new();

    assert_eq!(meta.expires_at, NO_EXPIRATION);

    meta.set_expiration(1000000);
    assert_eq!(meta.expires_at, 1000000);

    meta.clear_expiration();
    assert_eq!(meta.expires_at, NO_EXPIRATION);
  }

  #[test]
  fn test_set_metadata_default() {
    let meta = SetMetadata::default();
    assert_eq!(meta.flags, (CURRENT_VERSION << 4) | TYPE_SET);
    assert_eq!(meta.size, 0);
  }

  #[test]
  fn test_decode_error_invalid_data() {
    let valid_meta = SetMetadata::new();
    let encoded = valid_meta.serialize();

    if encoded.len() > 2 {
      assert_eq!(
        SetMetadata::deserialize(&encoded[..1]),
        Err(DecodeError::InvalidData)
      );
    }
  }

  // ==================== SetMemberValue Tests ====================

  #[test]
  fn test_set_member_value_serialize() {
    let val = SetMemberValue;
    let encoded = val.serialize();
    assert!(encoded.is_empty());
  }

  #[test]
  fn test_set_member_value_deserialize_empty() {
    let decoded = SetMemberValue::deserialize(b"").unwrap();
    assert_eq!(decoded, SetMemberValue);
  }

  #[test]
  fn test_set_member_value_deserialize_non_empty() {
    assert_eq!(
      SetMemberValue::deserialize(b"garbage"),
      Err(DecodeError::InvalidData)
    );
  }

  // ==================== Sub-key Tests ====================

  #[test]
  fn test_build_sub_key() {
    let key = b"myset";
    let version = 12345u64;
    let member = b"mymember";

    let sub_key = SetMemberValue::build_sub_key(key, version, member);

    // Verify structure: key_len(4) | key | version(8) | member
    let key_len = u32::from_be_bytes([sub_key[0], sub_key[1], sub_key[2], sub_key[3]]) as usize;
    assert_eq!(key_len, key.len());
    assert_eq!(&sub_key[4..4 + key_len], key);
    assert_eq!(
      &sub_key[4 + key_len..4 + key_len + 8],
      &version.to_be_bytes()
    );
    assert_eq!(&sub_key[4 + key_len + 8..], member);
  }

  #[test]
  fn test_parse_sub_key() {
    let key = b"myset";
    let version = 12345u64;
    let member = b"mymember";

    let sub_key = SetMemberValue::build_sub_key(key, version, member);
    let (parsed_key, parsed_version, parsed_member) =
      SetMemberValue::parse_sub_key(&sub_key).unwrap();

    assert_eq!(parsed_key, key);
    assert_eq!(parsed_version, version);
    assert_eq!(parsed_member, member);
  }

  #[test]
  fn test_parse_sub_key_empty_member() {
    let key = b"myset";
    let version = 12345u64;
    let member = b"";

    let sub_key = SetMemberValue::build_sub_key(key, version, member);
    let (parsed_key, parsed_version, parsed_member) =
      SetMemberValue::parse_sub_key(&sub_key).unwrap();

    assert_eq!(parsed_key, key);
    assert_eq!(parsed_version, version);
    assert_eq!(parsed_member, member);
  }

  #[test]
  fn test_parse_sub_key_empty_key() {
    let key = b"";
    let version = 12345u64;
    let member = b"mymember";

    let sub_key = SetMemberValue::build_sub_key(key, version, member);
    let (parsed_key, parsed_version, parsed_member) =
      SetMemberValue::parse_sub_key(&sub_key).unwrap();

    assert_eq!(parsed_key, key);
    assert_eq!(parsed_version, version);
    assert_eq!(parsed_member, member);
  }

  #[test]
  fn test_parse_sub_key_too_short() {
    let sub_key = vec![0u8; 11];
    assert!(SetMemberValue::parse_sub_key(&sub_key).is_none());

    let sub_key = vec![0u8; 7];
    assert!(SetMemberValue::parse_sub_key(&sub_key).is_none());
  }

  #[test]
  fn test_parse_sub_key_truncated_key() {
    let mut sub_key = Vec::new();
    sub_key.extend_from_slice(&100u32.to_be_bytes());
    sub_key.extend_from_slice(b"short");

    assert!(SetMemberValue::parse_sub_key(&sub_key).is_none());
  }

  #[test]
  fn test_build_sub_key_hex_roundtrip() {
    let key = b"myset";
    let version = 12345u64;
    let member = b"hello";

    let hex_str = SetMemberValue::build_sub_key_hex(key, version, member);
    let (parsed_key, parsed_version, parsed_member) =
      SetMemberValue::parse_sub_key_hex(&hex_str).unwrap();

    assert_eq!(parsed_key, key.to_vec());
    assert_eq!(parsed_version, version);
    assert_eq!(parsed_member, member.to_vec());
  }

  #[test]
  fn test_build_prefix_hex() {
    let key = b"myset";
    let version = 12345u64;

    let prefix_hex = SetMemberValue::build_prefix_hex(key, version);

    // Decode and verify: key_len(4) | key | version(8)
    let prefix_bytes = hex::decode(&prefix_hex).unwrap();
    assert_eq!(prefix_bytes.len(), 4 + 5 + 8);

    let key_len = u32::from_be_bytes([
      prefix_bytes[0],
      prefix_bytes[1],
      prefix_bytes[2],
      prefix_bytes[3],
    ]);
    assert_eq!(key_len as usize, 5);
    assert_eq!(&prefix_bytes[4..9], b"myset");

    let version_bytes = &prefix_bytes[9..17];
    assert_eq!(version_bytes, &version.to_be_bytes());
  }

  #[test]
  fn test_build_prefix_hex_is_valid_utf8() {
    let key = b"test";
    let version = 42u64;

    let prefix_hex = SetMemberValue::build_prefix_hex(key, version);
    assert!(prefix_hex.is_ascii());
    assert!(String::from_utf8(prefix_hex.into_bytes()).is_ok());
  }

  #[test]
  fn test_sub_key_ordering() {
    let key = b"myset";
    let version = 100u64;

    let sk_a = SetMemberValue::build_sub_key_hex(key, version, b"a");
    let sk_b = SetMemberValue::build_sub_key_hex(key, version, b"b");
    let sk_c = SetMemberValue::build_sub_key_hex(key, version, b"ab");

    // Members with the same prefix should be ordered lexicographically
    assert!(sk_a < sk_b);
    assert!(sk_a < sk_c);
    assert!(sk_c < sk_b);
  }

  #[test]
  fn test_different_keys_produce_different_prefixes() {
    let prefix_a = SetMemberValue::build_prefix_hex(b"set_a", 100);
    let prefix_b = SetMemberValue::build_prefix_hex(b"set_b", 100);

    assert_ne!(prefix_a, prefix_b);
  }

  #[test]
  fn test_different_versions_produce_different_prefixes() {
    let prefix_v1 = SetMemberValue::build_prefix_hex(b"myset", 100);
    let prefix_v2 = SetMemberValue::build_prefix_hex(b"myset", 200);

    assert_ne!(prefix_v1, prefix_v2);
  }
}
