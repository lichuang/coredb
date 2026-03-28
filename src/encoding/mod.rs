//! Value encoding/decoding for storage
//!
//! This module provides encoding and decoding for different data types
//! stored in the database.

pub mod hash;
pub mod list;
pub mod string;

/// Current format version for all encoded types (stored in high 4 bits of flags)
pub const CURRENT_VERSION: u8 = 1;

/// Data type constants (stored in low 4 bits of flags)
pub const TYPE_STRING: u8 = 0x01;
pub const TYPE_HASH: u8 = 0x02;
pub const TYPE_LIST: u8 = 0x03;

/// Special value indicating no expiration (0 means never expire)
pub const NO_EXPIRATION: u64 = 0;

pub use hash::{HashFieldValue, HashMetadata};
#[allow(unused_imports)]
pub use list::{ListElementValue, ListMetadata};
pub use string::StringValue;
