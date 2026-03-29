//! Bitmap type encoding/decoding for storage
//!
//! Bitmap data is stored in two parts:
//! 1. Metadata: stored at `key`, contains flags, expires_at, version, size
//! 2. Fragments: stored at `key|version|fragment_index`, each fragment is up to 1KiB (8192 bits)
//!
//! This design follows KVRocks' approach inspired by Linux virtual memory.
//! The bitmap is broken into 1KiB fragments for sparse efficiency.
//!
//! # Storage Layout
//!
//! ## Bitmap Metadata
//! ```text
//! +-----------+------------+-----------+-----------+
//! |   flags   | expires_at |  version  |   size    |
//! | (1byte)   |  (8byte)   |  (8byte)  |  (8byte)  |
//! +-----------+------------+-----------+-----------+
//! ```
//!
//! - `flags`: high 4 bits = encoding version, low 4 bits = data type (0x06 for bitmap)
//! - `expires_at`: expiration timestamp in milliseconds, 0 means no expiration
//! - `version`: used for fast deletion (increment to invalidate all fragments)
//! - `size`: total number of bits in the bitmap (max offset + 1)
//!
//! ## Bitmap Fragment
//! ```text
//!                          +---------------+
//! key|version|index    =>  |   fragment    |
//!                          +  (<=1024byte) |
//!                          +---------------+
//! ```
//!
//! - `fragment_index`: fragment number (bit_offset / 8192)
//! - `fragment_data`: raw bytes, up to 1024 bytes (8192 bits)
//! - Nonexistent fragments are treated as all zeros
//! - Fragment data uses LSB numbering (right-to-left within each byte)
//!
//! # Example
//!
//! After `SETBIT mybitmap 0 1`, `SETBIT mybitmap 8193 1`:
//! ```text
//! Metadata:  {flags:0x16, expires_at:0, version:V, size:8194}
//!
//! Fragments:
//!   key|V|0 => 0x01        (bit 0 set, 1024 bytes)
//!   key|V|1 => 0x02        (bit 1 set in fragment, 1024 bytes)
//! ```

use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::encoding::{CURRENT_VERSION, NO_EXPIRATION, TYPE_BITMAP};

/// Each fragment holds 1024 bytes = 8192 bits
pub const BITMAP_FRAGMENT_SIZE: usize = 1024;

/// Number of bits per fragment
pub const BITMAP_FRAGMENT_BITS: u64 = (BITMAP_FRAGMENT_SIZE * 8) as u64;

/// Bitmap metadata structure for storage
///
/// Stored at the user key in RocksDB. Tracks the total bit count.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BitmapMetadata {
  /// Flags field: high 4 bits = encoding version, low 4 bits = data type
  pub flags: u8,
  /// Expiration timestamp in milliseconds (Unix timestamp), 0 means no expiration
  pub expires_at: u64,
  /// Version for fast deletion (incremented on each recreation)
  pub version: u64,
  /// Total number of bits in the bitmap (max offset + 1)
  pub size: u64,
}

impl BitmapMetadata {
  /// Create a new empty BitmapMetadata without expiration
  pub fn new() -> Self {
    Self {
      flags: (CURRENT_VERSION << 4) | TYPE_BITMAP,
      expires_at: NO_EXPIRATION,
      version: Self::generate_version(),
      size: 0,
    }
  }

  /// Create a new BitmapMetadata with expiration timestamp (in milliseconds)
  #[allow(dead_code)]
  pub fn with_expiration(expires_at: u64) -> Self {
    Self {
      flags: (CURRENT_VERSION << 4) | TYPE_BITMAP,
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

  /// Check if this bitmap has expired (given current timestamp in milliseconds)
  pub fn is_expired(&self, now_ms: u64) -> bool {
    if self.expires_at == NO_EXPIRATION {
      return false;
    }
    now_ms >= self.expires_at
  }

  /// Check if this bitmap has an expiration time set
  #[allow(dead_code)]
  pub fn has_expiration(&self) -> bool {
    self.expires_at != NO_EXPIRATION
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

impl Default for BitmapMetadata {
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

/// Bitmap fragment sub-key builder/parser
///
/// Bitmap data is broken into fragments of 1KiB (8192 bits) each.
/// Each fragment is stored as a separate KV pair in RocksDB.
///
/// # Storage Layout
///
/// ```text
///                            +---------------+
/// key|version|index     =>  |   fragment    |
///                            + (<=1024byte) |
///                            +---------------+
/// ```
///
/// - `key`: the original bitmap key (user key)
/// - `version`: 8-byte version from BitmapMetadata
/// - `index`: fragment index = bit_offset / 8192
/// - `fragment`: raw bytes, up to 1024 bytes
pub struct BitmapFragment;

impl BitmapFragment {
  /// Build the sub-key for storage: key_len|key|version|fragment_index
  ///
  /// Format:
  /// ```text
  /// +-----------+-------------+-------------+-----------------+
  /// | key_len   |     key     |   version   | fragment_index  |
  /// | (4 bytes) |  (key_len)  |  (8 bytes)  |   (8 bytes)     |
  /// +-----------+-------------+-------------+-----------------+
  /// ```
  pub fn build_sub_key(key: &[u8], version: u64, fragment_index: u64) -> Vec<u8> {
    let key_len = key.len() as u32;
    let mut sub_key = Vec::with_capacity(4 + key.len() + 8 + 8);
    sub_key.extend_from_slice(&key_len.to_be_bytes());
    sub_key.extend_from_slice(key);
    sub_key.extend_from_slice(&version.to_be_bytes());
    sub_key.extend_from_slice(&fragment_index.to_be_bytes());
    sub_key
  }

  /// Build the sub-key as hex string for storage
  pub fn build_sub_key_hex(key: &[u8], version: u64, fragment_index: u64) -> String {
    let sub_key = Self::build_sub_key(key, version, fragment_index);
    hex::encode(&sub_key)
  }

  /// Build the hex-encoded prefix for scanning all fragments of a bitmap
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

  /// Parse a hex-encoded sub-key into its components: (key, version, fragment_index)
  #[allow(dead_code)]
  pub fn parse_sub_key_hex(hex_str: &str) -> Option<(Vec<u8>, u64, u64)> {
    let sub_key = hex::decode(hex_str).ok()?;
    let (key, version, fragment_index) = Self::parse_sub_key(&sub_key)?;
    Some((key.to_vec(), version, fragment_index))
  }

  /// Parse a sub-key into its components: (key, version, fragment_index)
  #[allow(dead_code)]
  pub fn parse_sub_key(sub_key: &[u8]) -> Option<(&[u8], u64, u64)> {
    // Need at least 4 + 0 + 8 + 8 = 20 bytes
    if sub_key.len() < 20 {
      return None;
    }

    let key_len = u32::from_be_bytes([sub_key[0], sub_key[1], sub_key[2], sub_key[3]]) as usize;

    // Check if we have enough bytes: 4 (key_len) + key_len (key) + 8 (version) + 8 (index)
    if sub_key.len() < 20 + key_len {
      return None;
    }

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
    let fragment_index = u64::from_be_bytes([
      index_bytes[0],
      index_bytes[1],
      index_bytes[2],
      index_bytes[3],
      index_bytes[4],
      index_bytes[5],
      index_bytes[6],
      index_bytes[7],
    ]);

    Some((key, version, fragment_index))
  }

  /// Get a bit from a fragment at the given offset within the fragment
  ///
  /// Uses LSB numbering: within a byte, bit 0 is the least significant bit.
  ///
  /// # Arguments
  /// * `fragment` - The fragment data (raw bytes)
  /// * `offset_in_fragment` - The bit offset within this fragment (0..8191)
  ///
  /// # Returns
  /// 0 or 1
  pub fn get_bit(fragment: &[u8], offset_in_fragment: u64) -> u8 {
    let byte_index = (offset_in_fragment / 8) as usize;
    let bit_index = offset_in_fragment % 8;

    if byte_index >= fragment.len() {
      return 0;
    }

    (fragment[byte_index] >> bit_index) & 1
  }

  /// Set a bit in a fragment at the given offset within the fragment
  ///
  /// Uses LSB numbering: within a byte, bit 0 is the least significant bit.
  ///
  /// # Arguments
  /// * `fragment` - The mutable fragment data
  /// * `offset_in_fragment` - The bit offset within this fragment (0..8191)
  /// * `value` - The bit value to set (0 or 1)
  pub fn set_bit(fragment: &mut Vec<u8>, offset_in_fragment: u64, value: u8) {
    let byte_index = (offset_in_fragment / 8) as usize;
    let bit_index = offset_in_fragment % 8;

    // Extend fragment if needed
    if byte_index >= fragment.len() {
      fragment.resize(byte_index + 1, 0);
    }

    if value == 1 {
      fragment[byte_index] |= 1 << bit_index;
    } else {
      fragment[byte_index] &= !(1 << bit_index);
    }
  }

  /// Calculate which fragment a given bit offset belongs to
  ///
  /// # Arguments
  /// * `bit_offset` - The global bit offset
  ///
  /// # Returns
  /// The fragment index
  pub fn fragment_index(bit_offset: u64) -> u64 {
    bit_offset / BITMAP_FRAGMENT_BITS
  }

  /// Calculate the offset within a fragment for a given global bit offset
  ///
  /// # Arguments
  /// * `bit_offset` - The global bit offset
  ///
  /// # Returns
  /// The offset within the fragment (0..8191)
  pub fn offset_in_fragment(bit_offset: u64) -> u64 {
    bit_offset % BITMAP_FRAGMENT_BITS
  }

  /// Create an empty fragment (all zeros)
  pub fn empty_fragment() -> Vec<u8> {
    vec![0u8; BITMAP_FRAGMENT_SIZE]
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::encoding::{NO_EXPIRATION, TYPE_BITMAP};

  // ==================== BitmapMetadata Tests ====================

  #[test]
  fn test_bitmap_metadata_new() {
    let meta = BitmapMetadata::new();
    assert_eq!(meta.flags, (CURRENT_VERSION << 4) | TYPE_BITMAP);
    assert_eq!(meta.expires_at, NO_EXPIRATION);
    assert_eq!(meta.size, 0);
  }

  #[test]
  fn test_bitmap_metadata_encode_decode() {
    let meta = BitmapMetadata::new();
    let encoded = meta.serialize();
    let decoded = BitmapMetadata::deserialize(&encoded).unwrap();
    assert_eq!(meta, decoded);
  }

  #[test]
  fn test_bitmap_metadata_with_expiration() {
    let meta = BitmapMetadata::with_expiration(1893456000000);
    let encoded = meta.serialize();
    let decoded = BitmapMetadata::deserialize(&encoded).unwrap();
    assert_eq!(meta, decoded);
    assert_eq!(decoded.expires_at, 1893456000000);
  }

  #[test]
  fn test_bitmap_metadata_with_size() {
    let mut meta = BitmapMetadata::new();
    meta.size = 8194;
    let encoded = meta.serialize();
    let decoded = BitmapMetadata::deserialize(&encoded).unwrap();
    assert_eq!(meta, decoded);
    assert_eq!(decoded.size, 8194);
  }

  #[test]
  fn test_bitmap_metadata_is_expired() {
    let mut meta = BitmapMetadata::new();
    assert!(!meta.is_expired(u64::MAX));
    assert!(!meta.has_expiration());

    meta.expires_at = 1000;
    assert!(meta.has_expiration());
    assert!(meta.is_expired(1000));
    assert!(meta.is_expired(1001));
    assert!(!meta.is_expired(999));
  }

  #[test]
  fn test_bitmap_metadata_get_type() {
    let meta = BitmapMetadata::new();
    assert_eq!(meta.get_type(), TYPE_BITMAP);
  }

  #[test]
  fn test_bitmap_metadata_expiration_operations() {
    let mut meta = BitmapMetadata::new();
    assert_eq!(meta.expires_at, NO_EXPIRATION);

    meta.set_expiration(1000000);
    assert_eq!(meta.expires_at, 1000000);

    meta.clear_expiration();
    assert_eq!(meta.expires_at, NO_EXPIRATION);
  }

  #[test]
  fn test_bitmap_metadata_default() {
    let meta = BitmapMetadata::default();
    assert_eq!(meta.flags, (CURRENT_VERSION << 4) | TYPE_BITMAP);
    assert_eq!(meta.size, 0);
  }

  #[test]
  fn test_decode_error_invalid_data() {
    let valid_meta = BitmapMetadata::new();
    let encoded = valid_meta.serialize();
    if encoded.len() > 2 {
      assert_eq!(
        BitmapMetadata::deserialize(&encoded[..1]),
        Err(DecodeError::InvalidData)
      );
    }
  }

  // ==================== BitmapFragment Tests ====================

  #[test]
  fn test_fragment_index() {
    assert_eq!(BitmapFragment::fragment_index(0), 0);
    assert_eq!(BitmapFragment::fragment_index(1), 0);
    assert_eq!(BitmapFragment::fragment_index(8191), 0);
    assert_eq!(BitmapFragment::fragment_index(8192), 1);
    assert_eq!(BitmapFragment::fragment_index(8193), 1);
    assert_eq!(BitmapFragment::fragment_index(16383), 1);
    assert_eq!(BitmapFragment::fragment_index(16384), 2);
    assert_eq!(BitmapFragment::fragment_index(100000), 12);
  }

  #[test]
  fn test_offset_in_fragment() {
    assert_eq!(BitmapFragment::offset_in_fragment(0), 0);
    assert_eq!(BitmapFragment::offset_in_fragment(1), 1);
    assert_eq!(BitmapFragment::offset_in_fragment(8191), 8191);
    assert_eq!(BitmapFragment::offset_in_fragment(8192), 0);
    assert_eq!(BitmapFragment::offset_in_fragment(8193), 1);
    assert_eq!(BitmapFragment::offset_in_fragment(16384), 0);
    assert_eq!(BitmapFragment::offset_in_fragment(100000), 100000 % 8192);
  }

  #[test]
  fn test_get_set_bit() {
    let mut fragment = vec![0u8; BITMAP_FRAGMENT_SIZE];

    // Set bit 0
    BitmapFragment::set_bit(&mut fragment, 0, 1);
    assert_eq!(BitmapFragment::get_bit(&fragment, 0), 1);
    assert_eq!(fragment[0], 0x01);

    // Set bit 7 (same byte, MSB in LSB numbering)
    BitmapFragment::set_bit(&mut fragment, 7, 1);
    assert_eq!(BitmapFragment::get_bit(&fragment, 7), 1);
    assert_eq!(fragment[0], 0x81);

    // Set bit 8 (next byte)
    BitmapFragment::set_bit(&mut fragment, 8, 1);
    assert_eq!(BitmapFragment::get_bit(&fragment, 8), 1);
    assert_eq!(fragment[1], 0x01);

    // Unset bit 0
    BitmapFragment::set_bit(&mut fragment, 0, 0);
    assert_eq!(BitmapFragment::get_bit(&fragment, 0), 0);
    assert_eq!(fragment[0], 0x80);

    // Bit not set should return 0
    assert_eq!(BitmapFragment::get_bit(&fragment, 3), 0);
  }

  #[test]
  fn test_get_bit_out_of_bounds() {
    let fragment = vec![0u8; 10];
    assert_eq!(BitmapFragment::get_bit(&fragment, 100), 0);
    assert_eq!(BitmapFragment::get_bit(&fragment, 8191), 0);
  }

  #[test]
  fn test_set_bit_extends_fragment() {
    let mut fragment = Vec::new();
    BitmapFragment::set_bit(&mut fragment, 0, 1);
    assert_eq!(fragment.len(), 1);
    assert_eq!(fragment[0], 0x01);

    BitmapFragment::set_bit(&mut fragment, 100, 1);
    assert!(fragment.len() > 1);
    assert_eq!(BitmapFragment::get_bit(&fragment, 100), 1);
  }

  #[test]
  fn test_empty_fragment() {
    let fragment = BitmapFragment::empty_fragment();
    assert_eq!(fragment.len(), BITMAP_FRAGMENT_SIZE);
    assert!(fragment.iter().all(|&b| b == 0));
  }

  #[test]
  fn test_build_sub_key() {
    let key = b"mybitmap";
    let version = 12345u64;
    let frag_idx = 5u64;

    let sub_key = BitmapFragment::build_sub_key(key, version, frag_idx);

    // Verify: key_len(4) | key | version(8) | fragment_index(8)
    let key_len = u32::from_be_bytes([sub_key[0], sub_key[1], sub_key[2], sub_key[3]]) as usize;
    assert_eq!(key_len, key.len());
    assert_eq!(&sub_key[4..4 + key_len], key);
    assert_eq!(
      &sub_key[4 + key_len..4 + key_len + 8],
      &version.to_be_bytes()
    );
    assert_eq!(
      &sub_key[4 + key_len + 8..4 + key_len + 16],
      &frag_idx.to_be_bytes()
    );
  }

  #[test]
  fn test_parse_sub_key() {
    let key = b"mybitmap";
    let version = 12345u64;
    let frag_idx = 5u64;

    let sub_key = BitmapFragment::build_sub_key(key, version, frag_idx);
    let (parsed_key, parsed_version, parsed_index) =
      BitmapFragment::parse_sub_key(&sub_key).unwrap();

    assert_eq!(parsed_key, key);
    assert_eq!(parsed_version, version);
    assert_eq!(parsed_index, frag_idx);
  }

  #[test]
  fn test_build_sub_key_hex_roundtrip() {
    let key = b"mybitmap";
    let version = 12345u64;
    let frag_idx = 5u64;

    let hex_str = BitmapFragment::build_sub_key_hex(key, version, frag_idx);
    let (parsed_key, parsed_version, parsed_index) =
      BitmapFragment::parse_sub_key_hex(&hex_str).unwrap();

    assert_eq!(parsed_key, key.to_vec());
    assert_eq!(parsed_version, version);
    assert_eq!(parsed_index, frag_idx);
  }

  #[test]
  fn test_build_prefix_hex() {
    let key = b"mybitmap";
    let version = 12345u64;

    let prefix_hex = BitmapFragment::build_prefix_hex(key, version);
    let prefix_bytes = hex::decode(&prefix_hex).unwrap();
    assert_eq!(prefix_bytes.len(), 4 + 8 + 8);

    let key_len = u32::from_be_bytes([
      prefix_bytes[0],
      prefix_bytes[1],
      prefix_bytes[2],
      prefix_bytes[3],
    ]);
    assert_eq!(key_len as usize, 8);
    assert_eq!(&prefix_bytes[4..12], b"mybitmap");

    let version_bytes = &prefix_bytes[12..20];
    assert_eq!(version_bytes, &version.to_be_bytes());
  }

  #[test]
  fn test_parse_sub_key_too_short() {
    let sub_key = vec![0u8; 19];
    assert!(BitmapFragment::parse_sub_key(&sub_key).is_none());
  }

  #[test]
  fn test_parse_sub_key_truncated_key() {
    let mut sub_key = Vec::new();
    sub_key.extend_from_slice(&100u32.to_be_bytes());
    sub_key.extend_from_slice(b"short");
    assert!(BitmapFragment::parse_sub_key(&sub_key).is_none());
  }

  #[test]
  fn test_fragment_ordering() {
    let key = b"mybitmap";
    let version = 100u64;

    let sk_0 = BitmapFragment::build_sub_key_hex(key, version, 0);
    let sk_1 = BitmapFragment::build_sub_key_hex(key, version, 1);
    let sk_10 = BitmapFragment::build_sub_key_hex(key, version, 10);

    assert!(sk_0 < sk_1);
    assert!(sk_1 < sk_10);
  }

  #[test]
  fn test_lsb_numbering() {
    let mut fragment = vec![0u8; BITMAP_FRAGMENT_SIZE];

    // In LSB numbering, bit 0 is the rightmost bit of byte 0
    BitmapFragment::set_bit(&mut fragment, 0, 1);
    assert_eq!(fragment[0], 0b00000001);

    BitmapFragment::set_bit(&mut fragment, 1, 1);
    assert_eq!(fragment[0], 0b00000011);

    BitmapFragment::set_bit(&mut fragment, 7, 1);
    assert_eq!(fragment[0], 0b10000011);

    // Verify each bit position
    assert_eq!(BitmapFragment::get_bit(&fragment, 0), 1);
    assert_eq!(BitmapFragment::get_bit(&fragment, 1), 1);
    assert_eq!(BitmapFragment::get_bit(&fragment, 2), 0);
    assert_eq!(BitmapFragment::get_bit(&fragment, 3), 0);
    assert_eq!(BitmapFragment::get_bit(&fragment, 4), 0);
    assert_eq!(BitmapFragment::get_bit(&fragment, 5), 0);
    assert_eq!(BitmapFragment::get_bit(&fragment, 6), 0);
    assert_eq!(BitmapFragment::get_bit(&fragment, 7), 1);
  }
}
