//! Value encoding/decoding for storage
//!
//! This module provides encoding and decoding for different data types
//! stored in the database.

pub mod hash;
pub mod string;

/// Current format version for all encoded types
pub const CURRENT_VERSION: u8 = 1;

/// Special value indicating no expiration (0 means never expire)
pub const NO_EXPIRATION: u64 = 0;

pub use hash::HashMetadata;
pub use string::StringValue;
