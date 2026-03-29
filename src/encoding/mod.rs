//! Value encoding/decoding for storage
//!
//! This module provides encoding and decoding for different data types
//! stored in the database.

pub mod bitmap;
pub mod bloomfilter;
pub mod hash;
pub mod hyperloglog;
pub mod json;
pub mod list;
pub mod set;
pub mod string;
pub mod zset;

/// Current format version for all encoded types (stored in high 4 bits of flags)
pub const CURRENT_VERSION: u8 = 1;

/// Data type constants (stored in low 4 bits of flags)
pub const TYPE_STRING: u8 = 0x01;
pub const TYPE_HASH: u8 = 0x02;
pub const TYPE_LIST: u8 = 0x03;
pub const TYPE_SET: u8 = 0x04;
pub const TYPE_ZSET: u8 = 0x05;
pub const TYPE_BITMAP: u8 = 0x06;
pub const TYPE_BLOOMFILTER: u8 = 0x09;
pub const TYPE_HYPERLOGLOG: u8 = 0x0B;
pub const TYPE_JSON: u8 = 0x0A;

/// Special value indicating no expiration (0 means never expire)
pub const NO_EXPIRATION: u64 = 0;

#[allow(unused_imports)]
pub use bitmap::{BITMAP_FRAGMENT_BITS, BITMAP_FRAGMENT_SIZE, BitmapFragment, BitmapMetadata};
#[allow(unused_imports)]
pub use bloomfilter::{BloomFilterMetadata, BloomFilterSubKey};
pub use hash::{HashFieldValue, HashMetadata};
#[allow(unused_imports)]
pub use hyperloglog::{HyperLogLogMetadata, HyperLogLogSubKey};
#[allow(unused_imports)]
pub use json::JsonMetadata;
pub use list::{ListElementValue, ListMetadata};
pub use set::{SetMemberValue, SetMetadata};
pub use string::StringValue;
pub use zset::{ZSetMemberValue, ZSetMetadata};
