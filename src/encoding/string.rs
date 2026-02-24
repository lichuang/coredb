//! String type encoding/decoding for storage

use rkyv::{Archive, Deserialize, Serialize};

/// String value structure for storage
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub struct StringValue {
    /// Format version
    pub version: u8,
    /// Optional expiration timestamp in milliseconds (Unix timestamp)
    pub expires_at: Option<u64>,
    /// Actual data
    pub data: Vec<u8>,
}

impl StringValue {
    /// Create a new StringValue without expiration
    pub fn new(data: impl Into<Vec<u8>>) -> Self {
        Self {
            version: super::CURRENT_VERSION,
            expires_at: None,
            data: data.into(),
        }
    }

    /// Create a new StringValue with expiration timestamp (in milliseconds)
    pub fn with_expiration(data: impl Into<Vec<u8>>, expires_at: u64) -> Self {
        Self {
            version: super::CURRENT_VERSION,
            expires_at: Some(expires_at),
            data: data.into(),
        }
    }

    /// Serialize to bytes using rkyv
    pub fn serialize(&self) -> Vec<u8> {
        rkyv::to_bytes::<rkyv::rancor::Error>(self)
            .expect("serialization should succeed")
            .into()
    }

    /// Deserialize from bytes using rkyv
    pub fn deserialize(bytes: &[u8]) -> Result<Self, DecodeError> {
        let archived = rkyv::access::<ArchivedStringValue, rkyv::rancor::Error>(bytes)
            .map_err(|_| DecodeError::InvalidData)?;
        
        rkyv::api::deserialize_using::<StringValue, _, rkyv::rancor::Error>(archived, &mut ())
            .map_err(|_| DecodeError::InvalidData)
    }

    /// Check if this value has expired (given current timestamp in milliseconds)
    pub fn is_expired(&self, now_ms: u64) -> bool {
        match self.expires_at {
            Some(exp) => now_ms >= exp,
            None => false,
        }
    }
}

/// Errors that can occur during decoding
#[derive(Debug, Clone, PartialEq)]
pub enum DecodeError {
    /// Input data is invalid or corrupted
    InvalidData,
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodeError::InvalidData => write!(f, "invalid data for decoding"),
        }
    }
}

impl std::error::Error for DecodeError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_without_expiration() {
        let value = StringValue::new(b"hello world");
        let encoded = value.serialize();
        let decoded = StringValue::deserialize(&encoded).unwrap();

        assert_eq!(value, decoded);
        assert_eq!(decoded.version, crate::encoding::CURRENT_VERSION);
        assert_eq!(decoded.expires_at, None);
        assert_eq!(decoded.data, b"hello world");
    }

    #[test]
    fn test_encode_decode_with_expiration() {
        let value = StringValue::with_expiration(b"hello world", 1893456000000);
        let encoded = value.serialize();
        let decoded = StringValue::deserialize(&encoded).unwrap();

        assert_eq!(value, decoded);
        assert_eq!(decoded.version, crate::encoding::CURRENT_VERSION);
        assert_eq!(decoded.expires_at, Some(1893456000000));
        assert_eq!(decoded.data, b"hello world");
    }

    #[test]
    fn test_empty_data() {
        let value = StringValue::new(b"");
        let encoded = value.serialize();
        let decoded = StringValue::deserialize(&encoded).unwrap();

        assert_eq!(value, decoded);
        assert!(decoded.data.is_empty());
    }

    #[test]
    fn test_large_data() {
        let data = vec![0u8; 10000];
        let value = StringValue::new(data.clone());
        let encoded = value.serialize();
        let decoded = StringValue::deserialize(&encoded).unwrap();

        assert_eq!(decoded.data, data);
    }

    #[test]
    fn test_is_expired() {
        let value = StringValue::with_expiration(b"data", 1000);
        assert!(value.is_expired(1000));
        assert!(value.is_expired(1001));
        assert!(!value.is_expired(999));

        let value_no_exp = StringValue::new(b"data");
        assert!(!value_no_exp.is_expired(u64::MAX));
    }

    #[test]
    fn test_decode_error_invalid_data() {
        // Invalid data should fail
        assert_eq!(
            StringValue::deserialize(b""),
            Err(DecodeError::InvalidData)
        );
        assert_eq!(
            StringValue::deserialize(b"garbage"),
            Err(DecodeError::InvalidData)
        );
    }

    #[test]
    fn test_version_compatibility() {
        // Test that we can decode data with different version numbers
        let value = StringValue {
            version: 42, // Some future version
            expires_at: None,
            data: b"test".to_vec(),
        };
        let encoded = value.serialize();
        let decoded = StringValue::deserialize(&encoded).unwrap();

        assert_eq!(decoded.version, 42);
    }
}
