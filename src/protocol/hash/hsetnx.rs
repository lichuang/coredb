//! HSETNX command implementation
//!
//! HSETNX key field value
//! Sets field in the hash stored at key to value, only if the field does not already exist.
//!
//! Returns:
//! - 1 if the field was set (it did not exist before)
//! - 0 if the field already exists and was not set
//!
//! Note: This command uses atomic batch write to ensure the field and metadata
//! are written together as a single atomic operation.

use rockraft::raft::types::UpsertKV;

use crate::encoding::{HashFieldValue, HashMetadata};
use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

/// HSETNX command handler
pub struct HSetNxCommand;

/// Parsed HSETNX arguments
#[derive(Debug)]
struct HSetNxArgs {
  key: String,
  field: Vec<u8>,
  value: Vec<u8>,
}

impl HSetNxCommand {
  /// Parse arguments from RESP items
  /// Format: HSETNX key field value (4 items total)
  fn parse_args(items: &[Value]) -> Result<HSetNxArgs, ProtocolError> {
    // HSETNX requires exactly 4 arguments: HSETNX key field value
    if items.len() != 4 {
      return Err(ProtocolError::WrongArgCount("hsetnx"));
    }

    // Parse key
    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::InvalidArgument("key")),
    };

    // Parse field
    let field = match &items[2] {
      Value::BulkString(Some(data)) => data.clone(),
      Value::SimpleString(s) => s.as_bytes().to_vec(),
      _ => return Err(ProtocolError::InvalidArgument("field")),
    };

    // Parse value
    let value = match &items[3] {
      Value::BulkString(Some(data)) => data.clone(),
      Value::SimpleString(s) => s.as_bytes().to_vec(),
      _ => return Err(ProtocolError::InvalidArgument("value")),
    };

    Ok(HSetNxArgs { key, field, value })
  }
}

#[async_trait]
impl Command for HSetNxCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    // Parse arguments
    let args = Self::parse_args(items)?;

    // Get or create metadata
    let mut metadata = match server.get(&args.key).await? {
      Some(raw_meta) => match HashMetadata::deserialize(&raw_meta) {
        Ok(meta) => {
          // Check if expired
          if meta.is_expired(now_ms()) {
            // Expired, treat as new
            HashMetadata::new()
          } else {
            meta
          }
        }
        Err(_) => {
          // Corrupted, create new
          HashMetadata::new()
        }
      },
      None => {
        // Not found, create new
        HashMetadata::new()
      }
    };

    let version = metadata.version;

    // Build sub_key for the field
    let sub_key_str = HashFieldValue::build_sub_key_hex(args.key.as_bytes(), version, &args.field);

    // Check if field exists
    let field_exists = (server.get(&sub_key_str).await?).is_some();

    // If field exists, return 0 (do not set)
    if field_exists {
      return Ok(Value::Integer(0));
    }

    // Field does not exist, update metadata size first
    metadata.incr_size();

    // Prepare batch write entries
    let field_value = HashFieldValue::new(args.value);
    let entries = vec![
      // Insert field
      UpsertKV::insert(sub_key_str, &field_value.serialize()),
      // Update metadata
      UpsertKV::insert(args.key.clone(), &metadata.serialize()),
    ];

    // Perform atomic batch write
    server.batch_write(entries).await?;

    // Return 1 to indicate the field was set
    Ok(Value::Integer(1))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_parse_args_basic() {
    // HSETNX key field value
    let items = vec![
      Value::SimpleString("HSETNX".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"field1".to_vec())),
      Value::BulkString(Some(b"value1".to_vec())),
    ];

    let args = HSetNxCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mykey");
    assert_eq!(args.field, b"field1");
    assert_eq!(args.value, b"value1");
  }

  #[test]
  fn test_parse_args_insufficient_args() {
    // HSETNX key (missing field and value)
    let items = vec![
      Value::SimpleString("HSETNX".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
    ];

    let result = HSetNxCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_too_many_args() {
    // HSETNX key field value extra
    let items = vec![
      Value::SimpleString("HSETNX".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"field1".to_vec())),
      Value::BulkString(Some(b"value1".to_vec())),
      Value::BulkString(Some(b"extra".to_vec())),
    ];

    let result = HSetNxCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_simple_string_values() {
    // Test with SimpleString values
    let items = vec![
      Value::SimpleString("HSETNX".to_string()),
      Value::SimpleString("mykey".to_string()),
      Value::SimpleString("field1".to_string()),
      Value::SimpleString("value1".to_string()),
    ];

    let args = HSetNxCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mykey");
    assert_eq!(args.field, b"field1");
    assert_eq!(args.value, b"value1");
  }

  #[test]
  fn test_parse_args_empty_field() {
    // HSETNX key "" value (empty field name)
    let items = vec![
      Value::SimpleString("HSETNX".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"".to_vec())),
      Value::BulkString(Some(b"value1".to_vec())),
    ];

    let args = HSetNxCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mykey");
    assert_eq!(args.field, b"");
    assert_eq!(args.value, b"value1");
  }

  #[test]
  fn test_parse_args_empty_value() {
    // HSETNX key field "" (empty value)
    let items = vec![
      Value::SimpleString("HSETNX".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"field1".to_vec())),
      Value::BulkString(Some(b"".to_vec())),
    ];

    let args = HSetNxCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mykey");
    assert_eq!(args.field, b"field1");
    assert_eq!(args.value, b"");
  }
}
