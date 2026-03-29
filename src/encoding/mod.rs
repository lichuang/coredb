//! Value encoding/decoding for storage
//!
//! This module provides encoding and decoding for different data types
//! stored in the database.

pub mod bitmap;
pub mod hash;
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

/// Special value indicating no expiration (0 means never expire)
pub const NO_EXPIRATION: u64 = 0;

#[allow(unused_imports)]
pub use bitmap::{BITMAP_FRAGMENT_BITS, BITMAP_FRAGMENT_SIZE, BitmapFragment, BitmapMetadata};
pub use hash::{HashFieldValue, HashMetadata};
#[allow(unused_imports)]
pub use list::{ListElementValue, ListMetadata};
#[allow(unused_imports)]
pub use set::{SetMemberValue, SetMetadata};
pub use string::StringValue;
#[allow(unused_imports)]
pub use zset::{ZSetMemberValue, ZSetMetadata};
