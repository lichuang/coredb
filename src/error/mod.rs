//! CoreDB error types
//!
//! This module provides unified error types for the entire CoreDB project,
//! using `thiserror` for ergonomic error handling.

// Some enum variants are defined for future use by commands not yet implemented.
#![allow(dead_code)]

use thiserror::Error;

use crate::protocol::resp::Value;

/// Protocol-related errors (RESP parsing, command validation)
#[derive(Error, Clone, Debug, PartialEq)]
pub enum ProtocolError {
  /// Invalid RESP format
  #[error("invalid RESP format: {0}")]
  InvalidFormat(String),

  /// Unknown command
  #[error("unknown command '{0}'")]
  UnknownCommand(String),

  /// Wrong number of arguments for a command
  #[error("ERR wrong number of arguments for '{0}' command")]
  WrongArgCount(&'static str),

  /// Invalid argument value
  #[error("ERR invalid {0}")]
  InvalidArgument(&'static str),

  /// Syntax error in command
  #[error("ERR syntax error")]
  SyntaxError,

  /// WRONGTYPE - operation against a key holding the wrong kind of value
  #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
  WrongType,

  /// Value is not a valid integer
  #[error("ERR value is not an integer or out of range")]
  NotAnInteger,

  /// Numeric overflow on increment/decrement
  #[error("ERR increment or decrement would overflow")]
  Overflow,

  /// Custom error with full Redis error message
  #[error("{0}")]
  Custom(&'static str),
}

/// Storage-related errors (Raft, RocksDB operations)
#[derive(Error, Clone, Debug, PartialEq)]
pub enum StorageError {
  /// Raft consensus error
  #[error("raft error: {0}")]
  Raft(String),

  /// Key not found
  #[error("key not found")]
  KeyNotFound,

  /// Failed to read from storage
  #[error("read failed: {0}")]
  ReadFailed(String),

  /// Failed to write to storage
  #[error("write failed: {0}")]
  WriteFailed(String),

  /// Failed to delete from storage
  #[error("delete failed: {0}")]
  DeleteFailed(String),
}

/// Encoding/decoding errors (postcard, data serialization)
#[derive(Error, Clone, Debug, PartialEq)]
pub enum EncodeError {
  /// Data corruption or invalid format
  #[error("invalid or corrupted data")]
  InvalidData,

  /// Serialization failed
  #[error("serialization failed: {0}")]
  SerializeFailed(String),

  /// Deserialization failed
  #[error("deserialization failed: {0}")]
  DeserializeFailed(String),

  /// Data version mismatch
  #[error("data version mismatch: expected {expected}, got {actual}")]
  VersionMismatch { expected: u8, actual: u8 },
}

/// Configuration errors
#[derive(Error, Clone, Debug, PartialEq)]
pub enum ConfigError {
  /// Invalid configuration value
  #[error("invalid configuration: {0}")]
  InvalidValue(String),

  /// Missing required configuration
  #[error("missing required configuration: {0}")]
  MissingValue(String),

  /// Failed to parse configuration file
  #[error("failed to parse config file: {0}")]
  ParseFailed(String),

  /// IO error while reading config
  #[error("io error: {0}")]
  Io(String),
}

/// Server/Network errors
#[derive(Error, Clone, Debug, PartialEq)]
pub enum ServerError {
  /// Failed to bind to address
  /// Connection error
  #[error("connection error: {0}")]
  Connection(String),

  /// Server is shutting down
  #[error("server is shutting down")]
  ShuttingDown,

  /// Failed to bind to address
  #[error("failed to bind to {addr}: {reason}")]
  BindFailed { addr: String, reason: String },
}

/// The main CoreDB error type
#[derive(Error, Clone, Debug, PartialEq)]
pub enum CoreDbError {
  /// Protocol-level error
  #[error(transparent)]
  Protocol(#[from] ProtocolError),

  /// Storage-level error
  #[error(transparent)]
  Storage(#[from] StorageError),

  /// Encoding/decoding error
  #[error(transparent)]
  Encode(#[from] EncodeError),

  /// Configuration error
  #[error(transparent)]
  Config(#[from] ConfigError),

  /// Server/Network error
  #[error(transparent)]
  Server(#[from] ServerError),
}

/// Type alias for CoreDB results
pub type CoreDbResult<T> = Result<T, CoreDbError>;

// === Conversions to RESP Value ===

impl From<CoreDbError> for Value {
  fn from(err: CoreDbError) -> Self {
    match &err {
      CoreDbError::Protocol(e) => Value::error(e.to_string()),
      // Protocol errors already contain the correct Redis error prefix
      CoreDbError::Storage(e) => Value::error(format!("ERR {}", e)),
      CoreDbError::Encode(e) => Value::error(format!("ERR {}", e)),
      CoreDbError::Config(e) => Value::error(format!("ERR {}", e)),
      CoreDbError::Server(e) => Value::error(format!("ERR {}", e)),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_protocol_error_display() {
    let err = ProtocolError::WrongArgCount("GET");
    assert_eq!(
      err.to_string(),
      "ERR wrong number of arguments for 'GET' command"
    );
  }

  #[test]
  fn test_protocol_error_wrong_type() {
    let err = ProtocolError::WrongType;
    assert_eq!(
      err.to_string(),
      "WRONGTYPE Operation against a key holding the wrong kind of value"
    );
  }

  #[test]
  fn test_protocol_error_not_integer() {
    let err = ProtocolError::NotAnInteger;
    assert_eq!(
      err.to_string(),
      "ERR value is not an integer or out of range"
    );
  }

  #[test]
  fn test_protocol_error_overflow() {
    let err = ProtocolError::Overflow;
    assert_eq!(err.to_string(), "ERR increment or decrement would overflow");
  }

  #[test]
  fn test_storage_error_display() {
    let err = StorageError::KeyNotFound;
    assert_eq!(err.to_string(), "key not found");
  }

  #[test]
  fn test_encode_error_display() {
    let err = EncodeError::InvalidData;
    assert_eq!(err.to_string(), "invalid or corrupted data");
  }

  #[test]
  fn test_core_db_error_from_protocol() {
    let protocol_err = ProtocolError::UnknownCommand("FOO".to_string());
    let core_err: CoreDbError = protocol_err.into();
    assert!(matches!(core_err, CoreDbError::Protocol(_)));
  }

  #[test]
  fn test_error_into_resp_value() {
    let err = ProtocolError::SyntaxError;
    let value: Value = CoreDbError::Protocol(err).into();
    match value {
      Value::Error(msg) => assert!(msg.contains("syntax error")),
      _ => panic!("Expected error value"),
    }
  }

  #[test]
  fn test_core_db_error_into_value() {
    let err = CoreDbError::Protocol(ProtocolError::WrongType);
    let value: Value = err.into();
    match value {
      Value::Error(msg) => {
        assert!(msg.starts_with("WRONGTYPE"));
      }
      _ => panic!("Expected error value"),
    }
  }

  #[test]
  fn test_storage_error_into_value_via_core() {
    let err = CoreDbError::Storage(StorageError::ReadFailed("test".to_string()));
    let value: Value = err.into();
    match value {
      Value::Error(msg) => {
        assert!(msg.starts_with("ERR"));
      }
      _ => panic!("Expected error value"),
    }
  }
}
