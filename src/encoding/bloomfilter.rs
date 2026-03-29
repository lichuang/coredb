//! Bloom Filter type encoding/decoding for storage
//!
//! Bloom Filter uses a layered (cascading) design with split block bloom filters.
//! When one layer gets full, a new layer is added with expanded capacity.
//!
//! # Storage Layout
//!
//! ## Bloom Filter Metadata
//! ```text
//! +-----------+------------+-----------+-----------+-------------------+-----------+---------------+------------+-------------+
//! |   flags   | expires_at |  version  |   size    | num_sub_filters   | expansion | base_capacity | error_rate | bloom_bytes  |
//! | (1byte)   |  (8byte)   |  (8byte)  |  (8byte)  |    (2byte)        |  (2byte)  |   (4byte)     |  (8byte)   |   (4byte)    |
//! +-----------+------------+-----------+-----------+-------------------+-----------+---------------+------------+-------------+
//! ```
//!
//! - `flags`: high 4 bits = encoding version, low 4 bits = data type (0x09)
//! - `expires_at`: expiration timestamp in milliseconds, 0 means no expiration
//! - `version`: used for fast deletion (increment to invalidate all sub-keys)
//! - `size`: number of elements added across all layers
//! - `num_sub_filters`: number of layers (sub-filters)
//! - `expansion`: expansion factor for each new layer (default 2)
//! - `base_capacity`: initial capacity of the first layer
//! - `error_rate`: target false positive rate (f64)
//! - `bloom_bytes`: number of bytes per split block bloom filter
//!
//! ## Bloom Filter Sub-keys
//! ```text
//!                           +---------------+
//! key|version|index    =>  |    filter     |
//!                           +  (bloom_bytes)|
//!                           +---------------+
//! ```
//!
//! - `index`: layer index (0, 1, 2, ...)
//! - `filter`: raw bloom filter bit data
//!
//! Each layer's capacity = base_capacity * (expansion ^ index).
//! The bloom_bytes is calculated based on capacity and error_rate:
//!   bloom_bytes = ceil(capacity * abs(ln(error_rate)) / (ln(2)^2 * 8))
//!
//! # Example
//!
//! After `BF.RESERVE mybf 0.01 1000` then `BF.ADD mybf hello`:
//! ```text
//! Metadata: {flags:0x19, expires_at:0, version:V, size:1,
//!            num_sub_filters:1, expansion:2, base_capacity:1000,
//!            error_rate:0.01, bloom_bytes:958}
//!
//! Sub-keys:
//!   key|V|0 => <958 bytes of bloom filter data>
//! ```

use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::encoding::{CURRENT_VERSION, NO_EXPIRATION, TYPE_BLOOMFILTER};

/// Default expansion factor when a new layer is needed
pub const DEFAULT_EXPANSION: u16 = 2;

/// Calculate bloom filter bytes from capacity and error rate
///
/// Uses the optimal formula: m = -n * ln(p) / (ln(2)^2)
/// where n = capacity, p = error_rate, m = bits, then convert to bytes.
pub fn calc_bloom_bytes(capacity: u32, error_rate: f64) -> u32 {
  if capacity == 0 || error_rate <= 0.0 || error_rate >= 1.0 {
    return 0;
  }
  let bits_per_elem = -(error_rate.ln()) / (2.0f64.ln().powi(2));
  let total_bits = (capacity as f64) * bits_per_elem;
  let total_bytes = (total_bits / 8.0).ceil() as u32;
  // Align to 64 bytes for efficiency
  total_bytes.div_ceil(64) * 64
}

/// Bloom Filter metadata structure for storage
///
/// Stored at the user key in RocksDB.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BloomFilterMetadata {
  /// Flags field: high 4 bits = encoding version, low 4 bits = data type
  pub flags: u8,
  /// Expiration timestamp in milliseconds (Unix timestamp), 0 means no expiration
  pub expires_at: u64,
  /// Version for fast deletion (incremented on each recreation)
  pub version: u64,
  /// Number of elements added across all layers
  pub size: u64,
  /// Number of sub-filters (layers)
  pub num_sub_filters: u16,
  /// Expansion factor for each new layer
  pub expansion: u16,
  /// Initial capacity of the first layer
  pub base_capacity: u32,
  /// Target false positive rate
  pub error_rate: f64,
  /// Number of bytes per bloom filter block
  pub bloom_bytes: u32,
}

#[allow(dead_code)]
impl BloomFilterMetadata {
  /// Create a new BloomFilterMetadata with given parameters
  pub fn new(error_rate: f64, capacity: u32, expansion: u16) -> Self {
    let bloom_bytes = calc_bloom_bytes(capacity, error_rate);
    Self {
      flags: (CURRENT_VERSION << 4) | TYPE_BLOOMFILTER,
      expires_at: NO_EXPIRATION,
      version: Self::generate_version(),
      size: 0,
      num_sub_filters: 1,
      expansion,
      base_capacity: capacity,
      error_rate,
      bloom_bytes,
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

  /// Check if this bloom filter has expired
  pub fn is_expired(&self, now_ms: u64) -> bool {
    if self.expires_at == NO_EXPIRATION {
      return false;
    }
    now_ms >= self.expires_at
  }

  /// Check if this bloom filter has an expiration time set
  #[allow(dead_code)]
  pub fn has_expiration(&self) -> bool {
    self.expires_at != NO_EXPIRATION
  }

  /// Set expiration timestamp
  #[allow(dead_code)]
  pub fn set_expiration(&mut self, expires_at: u64) {
    self.expires_at = expires_at;
  }

  /// Clear expiration
  #[allow(dead_code)]
  pub fn clear_expiration(&mut self) {
    self.expires_at = NO_EXPIRATION;
  }

  /// Get the type from flags (low 4 bits)
  pub fn get_type(&self) -> u8 {
    self.flags & 0x0F
  }

  /// Get the capacity of a specific layer (index)
  pub fn layer_capacity(&self, index: u16) -> u64 {
    self.base_capacity as u64 * (self.expansion as u64).pow(index as u32)
  }

  /// Increment the total element count
  pub fn incr_size(&mut self) {
    self.size += 1;
  }

  /// Add a new layer
  pub fn add_layer(&mut self) {
    let new_index = self.num_sub_filters;
    let capacity = self.layer_capacity(new_index);
    let new_bloom_bytes = calc_bloom_bytes(capacity as u32, self.error_rate);
    self.bloom_bytes = new_bloom_bytes;
    self.num_sub_filters += 1;
  }
}

impl Default for BloomFilterMetadata {
  fn default() -> Self {
    Self::new(0.01, 1000, DEFAULT_EXPANSION)
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

/// Bloom Filter sub-key builder/parser
///
/// Each bloom filter layer is stored as a separate KV pair.
///
/// # Storage Layout
///
/// ```text
///                             +---------------+
/// key|version|index     =>  |    filter     |
///                             + (bloom_bytes) |
///                             +---------------+
/// ```
///
/// - `key`: the original bloom filter key (user key)
/// - `version`: 8-byte version from BloomFilterMetadata
/// - `index`: layer index (0, 1, 2, ...)
/// - `filter`: raw bloom filter bit data
pub struct BloomFilterSubKey;

#[allow(dead_code)]
impl BloomFilterSubKey {
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

  /// Build the hex-encoded prefix for scanning all layers of a bloom filter
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
  pub fn parse_sub_key_hex(hex_str: &str) -> Option<(Vec<u8>, u64, u16)> {
    let sub_key = hex::decode(hex_str).ok()?;
    let (key, version, index) = Self::parse_sub_key(&sub_key)?;
    Some((key.to_vec(), version, index))
  }

  /// Parse a sub-key into its components: (key, version, index)
  #[allow(dead_code)]
  pub fn parse_sub_key(sub_key: &[u8]) -> Option<(&[u8], u64, u16)> {
    // Need at least 4 + 0 + 8 + 2 = 14 bytes
    if sub_key.len() < 14 {
      return None;
    }

    let key_len = u32::from_be_bytes([sub_key[0], sub_key[1], sub_key[2], sub_key[3]]) as usize;

    // Check: 4 (key_len) + key_len (key) + 8 (version) + 2 (index)
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
}

#[cfg(test)]
mod tests {
  use super::*;

  // ==================== calc_bloom_bytes Tests ====================

  #[test]
  fn test_calc_bloom_bytes_basic() {
    let bytes = calc_bloom_bytes(1000, 0.01);
    assert!(bytes > 0);
    // 1000 * 9.585 / 8 ≈ 1198, aligned to 64 = 1216
    assert_eq!(bytes % 64, 0);
  }

  #[test]
  fn test_calc_bloom_bytes_zero_capacity() {
    assert_eq!(calc_bloom_bytes(0, 0.01), 0);
  }

  #[test]
  fn test_calc_bloom_bytes_invalid_error_rate() {
    assert_eq!(calc_bloom_bytes(1000, 0.0), 0);
    assert_eq!(calc_bloom_bytes(1000, 1.0), 0);
    assert_eq!(calc_bloom_bytes(1000, -0.01), 0);
    assert_eq!(calc_bloom_bytes(1000, 2.0), 0);
  }

  #[test]
  fn test_calc_bloom_bytes_larger_capacity() {
    let bytes_small = calc_bloom_bytes(1000, 0.01);
    let bytes_large = calc_bloom_bytes(10000, 0.01);
    assert!(bytes_large > bytes_small);
  }

  #[test]
  fn test_calc_bloom_bytes_stricter_error_rate() {
    let bytes_01 = calc_bloom_bytes(1000, 0.01);
    let bytes_001 = calc_bloom_bytes(1000, 0.001);
    assert!(bytes_001 > bytes_01);
  }

  // ==================== BloomFilterMetadata Tests ====================

  #[test]
  fn test_bloom_metadata_new() {
    let meta = BloomFilterMetadata::new(0.01, 1000, 2);
    assert_eq!(meta.flags, (CURRENT_VERSION << 4) | TYPE_BLOOMFILTER);
    assert_eq!(meta.expires_at, NO_EXPIRATION);
    assert_eq!(meta.size, 0);
    assert_eq!(meta.num_sub_filters, 1);
    assert_eq!(meta.expansion, 2);
    assert_eq!(meta.base_capacity, 1000);
    assert!((meta.error_rate - 0.01).abs() < f64::EPSILON);
    assert!(meta.bloom_bytes > 0);
  }

  #[test]
  fn test_bloom_metadata_encode_decode() {
    let meta = BloomFilterMetadata::new(0.01, 1000, 2);
    let encoded = meta.serialize();
    let decoded = BloomFilterMetadata::deserialize(&encoded).unwrap();
    assert_eq!(meta, decoded);
  }

  #[test]
  fn test_bloom_metadata_with_expiration() {
    let mut meta = BloomFilterMetadata::new(0.01, 1000, 2);
    meta.set_expiration(1893456000000);
    let encoded = meta.serialize();
    let decoded = BloomFilterMetadata::deserialize(&encoded).unwrap();
    assert_eq!(meta, decoded);
    assert_eq!(decoded.expires_at, 1893456000000);
  }

  #[test]
  fn test_bloom_metadata_is_expired() {
    let mut meta = BloomFilterMetadata::new(0.01, 1000, 2);
    assert!(!meta.is_expired(u64::MAX));
    assert!(!meta.has_expiration());

    meta.expires_at = 1000;
    assert!(meta.has_expiration());
    assert!(meta.is_expired(1000));
    assert!(meta.is_expired(1001));
    assert!(!meta.is_expired(999));
  }

  #[test]
  fn test_bloom_metadata_get_type() {
    let meta = BloomFilterMetadata::new(0.01, 1000, 2);
    assert_eq!(meta.get_type(), TYPE_BLOOMFILTER);
  }

  #[test]
  fn test_bloom_metadata_layer_capacity() {
    let meta = BloomFilterMetadata::new(0.01, 1000, 2);
    assert_eq!(meta.layer_capacity(0), 1000);
    assert_eq!(meta.layer_capacity(1), 2000);
    assert_eq!(meta.layer_capacity(2), 4000);
    assert_eq!(meta.layer_capacity(3), 8000);
  }

  #[test]
  fn test_bloom_metadata_incr_size() {
    let mut meta = BloomFilterMetadata::new(0.01, 1000, 2);
    assert_eq!(meta.size, 0);
    meta.incr_size();
    assert_eq!(meta.size, 1);
    meta.incr_size();
    assert_eq!(meta.size, 2);
  }

  #[test]
  fn test_bloom_metadata_add_layer() {
    let mut meta = BloomFilterMetadata::new(0.01, 1000, 2);
    assert_eq!(meta.num_sub_filters, 1);
    let bytes_0 = meta.bloom_bytes;

    meta.add_layer();
    assert_eq!(meta.num_sub_filters, 2);
    // Layer 1 capacity is 2000, so bloom_bytes should be larger
    assert!(meta.bloom_bytes >= bytes_0);
  }

  #[test]
  fn test_bloom_metadata_expiration_operations() {
    let mut meta = BloomFilterMetadata::new(0.01, 1000, 2);
    assert_eq!(meta.expires_at, NO_EXPIRATION);

    meta.set_expiration(1000000);
    assert_eq!(meta.expires_at, 1000000);

    meta.clear_expiration();
    assert_eq!(meta.expires_at, NO_EXPIRATION);
  }

  #[test]
  fn test_bloom_metadata_default() {
    let meta = BloomFilterMetadata::default();
    assert_eq!(meta.flags, (CURRENT_VERSION << 4) | TYPE_BLOOMFILTER);
    assert_eq!(meta.size, 0);
  }

  #[test]
  fn test_decode_error_invalid_data() {
    let valid_meta = BloomFilterMetadata::new(0.01, 1000, 2);
    let encoded = valid_meta.serialize();
    if encoded.len() > 2 {
      assert_eq!(
        BloomFilterMetadata::deserialize(&encoded[..1]),
        Err(DecodeError::InvalidData)
      );
    }
  }

  // ==================== BloomFilterSubKey Tests ====================

  #[test]
  fn test_build_sub_key() {
    let key = b"mybf";
    let version = 12345u64;
    let index: u16 = 5;

    let sub_key = BloomFilterSubKey::build_sub_key(key, version, index);

    // Verify: key_len(4) | key | version(8) | index(2)
    let key_len = u32::from_be_bytes([sub_key[0], sub_key[1], sub_key[2], sub_key[3]]) as usize;
    assert_eq!(key_len, key.len());
    assert_eq!(&sub_key[4..4 + key_len], key);
    assert_eq!(
      &sub_key[4 + key_len..4 + key_len + 8],
      &version.to_be_bytes()
    );
    let idx_bytes = &sub_key[4 + key_len + 8..4 + key_len + 10];
    assert_eq!(idx_bytes, &index.to_be_bytes());
  }

  #[test]
  fn test_parse_sub_key() {
    let key = b"mybf";
    let version = 12345u64;
    let index: u16 = 5;

    let sub_key = BloomFilterSubKey::build_sub_key(key, version, index);
    let (parsed_key, parsed_version, parsed_index) =
      BloomFilterSubKey::parse_sub_key(&sub_key).unwrap();

    assert_eq!(parsed_key, key);
    assert_eq!(parsed_version, version);
    assert_eq!(parsed_index, index);
  }

  #[test]
  fn test_build_sub_key_hex_roundtrip() {
    let key = b"mybf";
    let version = 12345u64;
    let index: u16 = 5;

    let hex_str = BloomFilterSubKey::build_sub_key_hex(key, version, index);
    let (parsed_key, parsed_version, parsed_index) =
      BloomFilterSubKey::parse_sub_key_hex(&hex_str).unwrap();

    assert_eq!(parsed_key, key.to_vec());
    assert_eq!(parsed_version, version);
    assert_eq!(parsed_index, index);
  }

  #[test]
  fn test_build_prefix_hex() {
    let key = b"mybf";
    let version = 12345u64;

    let prefix_hex = BloomFilterSubKey::build_prefix_hex(key, version);
    let prefix_bytes = hex::decode(&prefix_hex).unwrap();
    assert_eq!(prefix_bytes.len(), 4 + 4 + 8);

    let key_len = u32::from_be_bytes([
      prefix_bytes[0],
      prefix_bytes[1],
      prefix_bytes[2],
      prefix_bytes[3],
    ]);
    assert_eq!(key_len as usize, 4);
    assert_eq!(&prefix_bytes[4..8], b"mybf");

    let version_bytes = &prefix_bytes[8..16];
    assert_eq!(version_bytes, &version.to_be_bytes());
  }

  #[test]
  fn test_parse_sub_key_too_short() {
    let sub_key = vec![0u8; 13];
    assert!(BloomFilterSubKey::parse_sub_key(&sub_key).is_none());
  }

  #[test]
  fn test_parse_sub_key_truncated_key() {
    let mut sub_key = Vec::new();
    sub_key.extend_from_slice(&100u32.to_be_bytes());
    sub_key.extend_from_slice(b"short");
    assert!(BloomFilterSubKey::parse_sub_key(&sub_key).is_none());
  }

  #[test]
  fn test_sub_key_ordering() {
    let key = b"mybf";
    let version = 100u64;

    let sk_0 = BloomFilterSubKey::build_sub_key_hex(key, version, 0);
    let sk_1 = BloomFilterSubKey::build_sub_key_hex(key, version, 1);
    let sk_10 = BloomFilterSubKey::build_sub_key_hex(key, version, 10);

    assert!(sk_0 < sk_1);
    assert!(sk_1 < sk_10);
  }

  #[test]
  fn test_sub_key_max_index() {
    let key = b"mybf";
    let version = 100u64;
    let index: u16 = u16::MAX;

    let sub_key = BloomFilterSubKey::build_sub_key(key, version, index);
    let (_, _, parsed_index) = BloomFilterSubKey::parse_sub_key(&sub_key).unwrap();
    assert_eq!(parsed_index, u16::MAX);
  }
}
