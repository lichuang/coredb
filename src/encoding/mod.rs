//! Value encoding/decoding for storage
//!
//! This module provides encoding and decoding for different data types
//! stored in the database.

pub mod string;

/// Current format version for all encoded types
pub const CURRENT_VERSION: u8 = 1;

pub use string::StringValue;
