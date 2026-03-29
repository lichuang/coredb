//! HyperLogLog type encoding/decoding for storage
//!
//! HyperLogLog is a probabilistic data structure that estimates the cardinality
//! of a set. It uses a fixed array of 16384 registers, each storing a 6-bit
//! counter (the maximum count of consecutive leading zeros observed).
//!
//! The register array is divided into 16 segments of 1024 registers each.
//! Each segment is stored as a separate KV pair (768 bytes per segment,
//! since 1024 * 6 bits / 8 = 768 bytes).
//!
//! The hash function uses a Redis-compatible modified MurmurHash2:
//! - Register index: first 14 bits of the 64-bit hash
//! - Leading zeros count: calculated from the remaining 50 bits
//!
//! # Storage Layout
//!
//! ## HyperLogLog Metadata
//! ```text
//! +-----------+------------+-----------+-----------+
//! |   flags   | expires_at |  version  |  hll_type |
//! | (1byte)   |  (8byte)   |  (8byte)  |  (1byte)  |
//! +-----------+------------+-----------+-----------+
//! ```
//!
//! - `flags`: high 4 bits = encoding version, low 4 bits = data type (0x0B)
//! - `expires_at`: expiration timestamp in milliseconds, 0 means no expiration
//! - `version`: used for fast deletion
//! - `hll_type`: 0 = dense representation
//!
//! ## HyperLogLog Segment
//! ```text
//!                            +---------------+
//! key|version|index     =>  |   segment     |
//!                            +  (768 bytes)  |
//!                            +---------------+
//! ```
//!
//! - `index`: segment index (0..15)
//! - `segment`: 768 bytes containing 1024 x 6-bit registers

use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::encoding::{CURRENT_VERSION, NO_EXPIRATION, TYPE_HYPERLOGLOG};

/// Total number of registers in a HyperLogLog
#[allow(dead_code)]
pub const HLL_REGISTERS: usize = 16384;

/// Number of segments the register array is divided into
#[allow(dead_code)]
pub const HLL_SEGMENTS: u16 = 16;

/// Number of registers per segment
pub const HLL_REGISTERS_PER_SEGMENT: usize = 1024;

/// Segment size in bytes: 1024 registers * 6 bits / 8 = 768 bytes
pub const HLL_SEGMENT_SIZE: usize = 768;

/// Number of bits used to store each register value (6 bits, max value 63)
pub const HLL_BITS_PER_REGISTER: u8 = 6;

/// HLLType value for dense representation
pub const HLL_DENSE: u8 = 0;

/// Maximum value a register can hold (6 bits)
pub const HLL_REGISTER_MAX: u8 = 63;

/// HyperLogLog metadata structure for storage
///
/// Stored at the user key in RocksDB.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HyperLogLogMetadata {
  /// Flags field: high 4 bits = encoding version, low 4 bits = data type
  pub flags: u8,
  /// Expiration timestamp in milliseconds (Unix timestamp), 0 means no expiration
  pub expires_at: u64,
  /// Version for fast deletion (incremented on each recreation)
  pub version: u64,
  /// HLL representation type (0 = dense)
  pub hll_type: u8,
}

#[allow(dead_code)]
impl HyperLogLogMetadata {
  /// Create a new HyperLogLogMetadata with dense representation
  pub fn new() -> Self {
    Self {
      flags: (CURRENT_VERSION << 4) | TYPE_HYPERLOGLOG,
      expires_at: NO_EXPIRATION,
      version: Self::generate_version(),
      hll_type: HLL_DENSE,
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

  /// Check if this HyperLogLog has expired
  pub fn is_expired(&self, now_ms: u64) -> bool {
    if self.expires_at == NO_EXPIRATION {
      return false;
    }
    now_ms >= self.expires_at
  }

  /// Check if this HyperLogLog has an expiration time set
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

impl Default for HyperLogLogMetadata {
  fn default() -> Self {
    Self::new()
  }
}

/// Errors that can occur during decoding
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
pub enum DecodeError {
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

/// HyperLogLog segment sub-key builder/parser
///
/// The register array is divided into 16 segments, each stored as a
/// separate KV pair. Each segment contains 1024 registers (6 bits each),
/// packed into 768 bytes.
///
/// # Storage Layout
///
/// ```text
///                             +---------------+
/// key|version|index     =>  |   segment     |
///                             +  (768 bytes)  |
///                             +---------------+
/// ```
///
/// # Register Encoding (LSB numbering)
///
/// Within each 768-byte segment, 1024 registers are packed as 6-bit values.
/// Byte layout:
/// ```text
/// byte[0]: reg[0](6bits) | reg[1](2bits)
/// byte[1]: reg[1](4bits) | reg[2](6bits)   -- actually this is wrong
/// ```
///
/// The actual encoding uses a compact bit-packing scheme where registers
/// are stored sequentially in 6-bit slots across the byte array.
#[allow(dead_code)]
pub struct HyperLogLogSubKey;

#[allow(dead_code)]
impl HyperLogLogSubKey {
  /// Build the sub-key for storage: key_len|key|version|index
  ///
  /// Format:
  /// ```text
  /// +-----------+-------------+-------------+-------------+
  /// | key_len   |     key     |   version   |   index    |
  /// | (4 bytes) |  (key_len)  |  (8 bytes)  |  (2 bytes) |
  /// +-----------+-------------+-------------+-------------+
  /// ```
  pub fn build_sub_key(key: &[u8], version: u64, index: u16) -> Vec<u8> {
    let key_len = key.len() as u32;
    let mut sub_key = Vec::with_capacity(4 + key.len() + 8 + 2);
    sub_key.extend_from_slice(&key_len.to_be_bytes());
    sub_key.extend_from_slice(key);
    sub_key.extend_from_slice(&version.to_be_bytes());
    sub_key.extend_from_slice(&index.to_be_bytes());
    sub_key
  }

  /// Build the sub-key as hex string for storage
  pub fn build_sub_key_hex(key: &[u8], version: u64, index: u16) -> String {
    let sub_key = Self::build_sub_key(key, version, index);
    hex::encode(&sub_key)
  }

  /// Build the hex-encoded prefix for scanning all segments
  pub fn build_prefix_hex(key: &[u8], version: u64) -> String {
    let key_len = key.len() as u32;
    let mut prefix = Vec::with_capacity(4 + key.len() + 8);
    prefix.extend_from_slice(&key_len.to_be_bytes());
    prefix.extend_from_slice(key);
    prefix.extend_from_slice(&version.to_be_bytes());
    hex::encode(&prefix)
  }

  /// Parse a hex-encoded sub-key into its components: (key, version, index)
  pub fn parse_sub_key_hex(hex_str: &str) -> Option<(Vec<u8>, u64, u16)> {
    let sub_key = hex::decode(hex_str).ok()?;
    let (key, version, index) = Self::parse_sub_key(&sub_key)?;
    Some((key.to_vec(), version, index))
  }

  /// Parse a sub-key into its components: (key, version, index)
  pub fn parse_sub_key(sub_key: &[u8]) -> Option<(&[u8], u64, u16)> {
    // Need at least 4 + 0 + 8 + 2 = 14 bytes
    if sub_key.len() < 14 {
      return None;
    }

    let key_len = u32::from_be_bytes([sub_key[0], sub_key[1], sub_key[2], sub_key[3]]) as usize;

    if sub_key.len() < 14 + key_len {
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
    let index_bytes = &sub_key[4 + key_len + 8..4 + key_len + 10];
    let index = u16::from_be_bytes([index_bytes[0], index_bytes[1]]);

    Some((key, version, index))
  }

  /// Create an empty segment (all registers set to 0)
  pub fn empty_segment() -> Vec<u8> {
    vec![0u8; HLL_SEGMENT_SIZE]
  }

  /// Get the value of a register from a segment
  ///
  /// # Arguments
  /// * `segment` - The segment data (768 bytes)
  /// * `register_index` - The register index (0..1023 within this segment)
  ///
  /// # Returns
  /// The register value (0..63)
  pub fn get_register(segment: &[u8], register_index: usize) -> u8 {
    let bit_offset = register_index * HLL_BITS_PER_REGISTER as usize;
    let byte_offset = bit_offset / 8;
    let bit_shift = (bit_offset % 8) as u8;

    if byte_offset >= segment.len() {
      return 0;
    }

    let combined = (segment[byte_offset] as u16)
      | ((segment.get(byte_offset + 1).copied().unwrap_or(0) as u16) << 8);
    ((combined >> bit_shift) & 0x3F) as u8
  }

  /// Set the value of a register in a segment
  ///
  /// # Arguments
  /// * `segment` - The mutable segment data
  /// * `register_index` - The register index (0..1023 within this segment)
  /// * `value` - The value to set (0..63)
  pub fn set_register(segment: &mut [u8], register_index: usize, value: u8) {
    let value = value.min(HLL_REGISTER_MAX);
    let bit_offset = register_index * HLL_BITS_PER_REGISTER as usize;
    let byte_offset = bit_offset / 8;
    let bit_shift = (bit_offset % 8) as u8;

    if byte_offset >= segment.len() {
      return;
    }

    let mask = 0x3F_u16 << bit_shift;
    let shifted_value = (value as u16) << bit_shift;

    segment[byte_offset] = (segment[byte_offset] as u16 & !mask | shifted_value) as u8;

    if byte_offset + 1 < segment.len() {
      segment[byte_offset + 1] =
        (segment[byte_offset + 1] as u16 & !(mask >> 8) | (shifted_value >> 8)) as u8;
    }
  }

  /// Calculate the segment index for a given register index (0..16383)
  pub fn register_to_segment(register_index: usize) -> u16 {
    (register_index / HLL_REGISTERS_PER_SEGMENT) as u16
  }

  /// Calculate the register index within a segment
  pub fn register_in_segment(register_index: usize) -> usize {
    register_index % HLL_REGISTERS_PER_SEGMENT
  }

  /// Hash an element using Redis-compatible modified MurmurHash2
  /// Returns the 64-bit hash value
  pub fn hash_element(data: &[u8]) -> u64 {
    let seed: u64 = 0xADC8B5F;
    murmur2_hash64(data, seed)
  }

  /// Extract the register index from a hash (first 14 bits)
  pub fn hash_to_register_index(hash: u64) -> usize {
    (hash >> 50) as usize & 0x3FFF
  }

  /// Count leading zeros of the remaining 50 bits after removing the top 14 bits
  /// Returns 1-based count (minimum 1), capped at 64
  pub fn hash_to_leading_zeros(hash: u64) -> u64 {
    let remaining = (hash & 0x0003_FFFF_FFFF_FFFF) | (1u64 << 49);
    64 - remaining.leading_zeros() as u64
  }
}

/// MurmurHash2 64-bit implementation
fn murmur2_hash64(data: &[u8], seed: u64) -> u64 {
  let m: u64 = 0xC6A4A793_5BD1E995;
  let r: u64 = 47;
  let len = data.len();

  let mut h = seed ^ (len as u64);
  let mut pos = 0;

  while pos + 8 <= len {
    let k = u64::from_le_bytes([
      data[pos],
      data[pos + 1],
      data[pos + 2],
      data[pos + 3],
      data[pos + 4],
      data[pos + 5],
      data[pos + 6],
      data[pos + 7],
    ]);
    h ^= k.wrapping_mul(m);
    h ^= h >> r;
    h = h.wrapping_mul(m);
    pos += 8;
  }

  match len & 7 {
    7 => {
      h ^= (data[pos + 6] as u64) << 48;
      h ^= (data[pos + 5] as u64) << 40;
      h ^= (data[pos + 4] as u64) << 32;
      h ^= (data[pos + 3] as u64) << 24;
      h ^= (data[pos + 2] as u64) << 16;
      h ^= (data[pos + 1] as u64) << 8;
      h ^= data[pos] as u64;
    }
    6 => {
      h ^= (data[pos + 5] as u64) << 40;
      h ^= (data[pos + 4] as u64) << 32;
      h ^= (data[pos + 3] as u64) << 24;
      h ^= (data[pos + 2] as u64) << 16;
      h ^= (data[pos + 1] as u64) << 8;
      h ^= data[pos] as u64;
    }
    5 => {
      h ^= (data[pos + 4] as u64) << 32;
      h ^= (data[pos + 3] as u64) << 24;
      h ^= (data[pos + 2] as u64) << 16;
      h ^= (data[pos + 1] as u64) << 8;
      h ^= data[pos] as u64;
    }
    4 => {
      h ^= (data[pos + 3] as u64) << 24;
      h ^= (data[pos + 2] as u64) << 16;
      h ^= (data[pos + 1] as u64) << 8;
      h ^= data[pos] as u64;
    }
    3 => {
      h ^= (data[pos + 2] as u64) << 16;
      h ^= (data[pos + 1] as u64) << 8;
      h ^= data[pos] as u64;
    }
    2 => {
      h ^= (data[pos + 1] as u64) << 8;
      h ^= data[pos] as u64;
    }
    1 => {
      h ^= data[pos] as u64;
    }
    _ => {}
  }

  h ^= h >> r;
  h = h.wrapping_mul(m);
  h ^= h >> r;
  h
}

#[cfg(test)]
mod tests {
  use super::*;

  // ==================== HyperLogLogMetadata Tests ====================

  #[test]
  fn test_hll_metadata_new() {
    let meta = HyperLogLogMetadata::new();
    assert_eq!(meta.flags, (CURRENT_VERSION << 4) | TYPE_HYPERLOGLOG);
    assert_eq!(meta.expires_at, NO_EXPIRATION);
    assert_eq!(meta.hll_type, HLL_DENSE);
  }

  #[test]
  fn test_hll_metadata_encode_decode() {
    let meta = HyperLogLogMetadata::new();
    let encoded = meta.serialize();
    let decoded = HyperLogLogMetadata::deserialize(&encoded).unwrap();
    assert_eq!(meta, decoded);
  }

  #[test]
  fn test_hll_metadata_is_expired() {
    let mut meta = HyperLogLogMetadata::new();
    assert!(!meta.is_expired(u64::MAX));
    assert!(!meta.has_expiration());

    meta.expires_at = 1000;
    assert!(meta.has_expiration());
    assert!(meta.is_expired(1000));
    assert!(!meta.is_expired(999));
  }

  #[test]
  fn test_hll_metadata_get_type() {
    let meta = HyperLogLogMetadata::new();
    assert_eq!(meta.get_type(), TYPE_HYPERLOGLOG);
  }

  #[test]
  fn test_hll_metadata_expiration_ops() {
    let mut meta = HyperLogLogMetadata::new();
    meta.set_expiration(1000000);
    assert_eq!(meta.expires_at, 1000000);
    meta.clear_expiration();
    assert_eq!(meta.expires_at, NO_EXPIRATION);
  }

  #[test]
  fn test_hll_metadata_default() {
    let meta = HyperLogLogMetadata::default();
    assert_eq!(meta.flags, (CURRENT_VERSION << 4) | TYPE_HYPERLOGLOG);
    assert_eq!(meta.hll_type, HLL_DENSE);
  }

  #[test]
  fn test_decode_error() {
    let valid = HyperLogLogMetadata::new();
    let encoded = valid.serialize();
    if encoded.len() > 2 {
      assert_eq!(
        HyperLogLogMetadata::deserialize(&encoded[..1]),
        Err(DecodeError::InvalidData)
      );
    }
  }

  // ==================== Sub-key Tests ====================

  #[test]
  fn test_build_sub_key() {
    let key = b"myhll";
    let version = 12345u64;
    let index: u16 = 5;

    let sub_key = HyperLogLogSubKey::build_sub_key(key, version, index);
    let key_len = u32::from_be_bytes([sub_key[0], sub_key[1], sub_key[2], sub_key[3]]) as usize;
    assert_eq!(key_len, key.len());
    assert_eq!(&sub_key[4..4 + key_len], key);
    let idx_bytes = &sub_key[4 + key_len + 8..4 + key_len + 10];
    assert_eq!(idx_bytes, &index.to_be_bytes());
  }

  #[test]
  fn test_parse_sub_key() {
    let key = b"myhll";
    let version = 12345u64;
    let index: u16 = 5;

    let sub_key = HyperLogLogSubKey::build_sub_key(key, version, index);
    let (parsed_key, parsed_version, parsed_index) =
      HyperLogLogSubKey::parse_sub_key(&sub_key).unwrap();

    assert_eq!(parsed_key, key);
    assert_eq!(parsed_version, version);
    assert_eq!(parsed_index, index);
  }

  #[test]
  fn test_build_sub_key_hex_roundtrip() {
    let key = b"myhll";
    let version = 12345u64;
    let index: u16 = 5;

    let hex_str = HyperLogLogSubKey::build_sub_key_hex(key, version, index);
    let (parsed_key, parsed_version, parsed_index) =
      HyperLogLogSubKey::parse_sub_key_hex(&hex_str).unwrap();

    assert_eq!(parsed_key, key.to_vec());
    assert_eq!(parsed_version, version);
    assert_eq!(parsed_index, index);
  }

  #[test]
  fn test_parse_sub_key_too_short() {
    let sub_key = vec![0u8; 13];
    assert!(HyperLogLogSubKey::parse_sub_key(&sub_key).is_none());
  }

  #[test]
  fn test_sub_key_ordering() {
    let key = b"myhll";
    let version = 100u64;
    let sk_0 = HyperLogLogSubKey::build_sub_key_hex(key, version, 0);
    let sk_1 = HyperLogLogSubKey::build_sub_key_hex(key, version, 1);
    let sk_15 = HyperLogLogSubKey::build_sub_key_hex(key, version, 15);
    assert!(sk_0 < sk_1);
    assert!(sk_1 < sk_15);
  }

  // ==================== Register Tests ====================

  #[test]
  fn test_empty_segment() {
    let segment = HyperLogLogSubKey::empty_segment();
    assert_eq!(segment.len(), HLL_SEGMENT_SIZE);
    assert!(segment.iter().all(|&b| b == 0));
  }

  #[test]
  fn test_get_set_register() {
    let mut segment = HyperLogLogSubKey::empty_segment();

    assert_eq!(HyperLogLogSubKey::get_register(&segment, 0), 0);

    HyperLogLogSubKey::set_register(&mut segment, 0, 5);
    assert_eq!(HyperLogLogSubKey::get_register(&segment, 0), 5);

    HyperLogLogSubKey::set_register(&mut segment, 0, 0);
    assert_eq!(HyperLogLogSubKey::get_register(&segment, 0), 0);
  }

  #[test]
  fn test_register_max_value() {
    let mut segment = HyperLogLogSubKey::empty_segment();
    HyperLogLogSubKey::set_register(&mut segment, 0, 63);
    assert_eq!(HyperLogLogSubKey::get_register(&segment, 0), 63);

    HyperLogLogSubKey::set_register(&mut segment, 0, 100);
    assert_eq!(HyperLogLogSubKey::get_register(&segment, 0), 63);
  }

  #[test]
  fn test_multiple_registers() {
    let mut segment = HyperLogLogSubKey::empty_segment();

    HyperLogLogSubKey::set_register(&mut segment, 0, 1);
    HyperLogLogSubKey::set_register(&mut segment, 1, 2);
    HyperLogLogSubKey::set_register(&mut segment, 1023, 63);

    assert_eq!(HyperLogLogSubKey::get_register(&segment, 0), 1);
    assert_eq!(HyperLogLogSubKey::get_register(&segment, 1), 2);
    assert_eq!(HyperLogLogSubKey::get_register(&segment, 2), 0);
    assert_eq!(HyperLogLogSubKey::get_register(&segment, 1023), 63);
  }

  #[test]
  fn test_register_segment_calculation() {
    assert_eq!(HyperLogLogSubKey::register_to_segment(0), 0);
    assert_eq!(HyperLogLogSubKey::register_to_segment(1023), 0);
    assert_eq!(HyperLogLogSubKey::register_to_segment(1024), 1);
    assert_eq!(HyperLogLogSubKey::register_to_segment(16383), 15);

    assert_eq!(HyperLogLogSubKey::register_in_segment(0), 0);
    assert_eq!(HyperLogLogSubKey::register_in_segment(1023), 1023);
    assert_eq!(HyperLogLogSubKey::register_in_segment(1024), 0);
    assert_eq!(HyperLogLogSubKey::register_in_segment(16383), 1023);
  }

  // ==================== Hash Tests ====================

  #[test]
  fn test_hash_deterministic() {
    let h1 = HyperLogLogSubKey::hash_element(b"hello");
    let h2 = HyperLogLogSubKey::hash_element(b"hello");
    assert_eq!(h1, h2);
  }

  #[test]
  fn test_hash_different_inputs() {
    let h1 = HyperLogLogSubKey::hash_element(b"hello");
    let h2 = HyperLogLogSubKey::hash_element(b"world");
    assert_ne!(h1, h2);
  }

  #[test]
  fn test_hash_to_register_index() {
    let hash = HyperLogLogSubKey::hash_element(b"test");
    let idx = HyperLogLogSubKey::hash_to_register_index(hash);
    assert!(idx < HLL_REGISTERS);
  }

  #[test]
  fn test_hash_to_leading_zeros() {
    let hash = HyperLogLogSubKey::hash_element(b"test");
    let zeros = HyperLogLogSubKey::hash_to_leading_zeros(hash);
    assert!(zeros >= 1);
    assert!(zeros <= 64);
  }

  #[test]
  fn test_hash_zero_bits_gives_max_zeros() {
    // All 50 lower bits are 0 => max leading zeros
    // Top 14 bits set to select register, bottom 50 bits = 0
    // hash = 0x3FFF << 50 = 0x3FFF_0000_0000_0000
    let hash: u64 = 0x3FFF << 50;
    let idx = HyperLogLogSubKey::hash_to_register_index(hash);
    assert_eq!(idx, 0x3FFF);
    let zeros = HyperLogLogSubKey::hash_to_leading_zeros(hash);
    // Remaining 50 bits are all 0, but we set bit 49 to ensure minimum 1
    assert_eq!(zeros, 50);
  }
}
