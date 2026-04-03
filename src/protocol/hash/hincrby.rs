//! HINCRBY command implementation
//!
//! HINCRBY key field increment
//! Increments the integer value of a field in a hash by a number.
//! Uses 0 as initial value if the field doesn't exist.

use rockraft::raft::types::UpsertKV;

use crate::encoding::{HashFieldValue, HashMetadata};
use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

/// Parsed HINCRBY arguments
#[derive(Debug)]
struct HIncrByArgs {
  key: String,
  field: Vec<u8>,
  increment: i64,
}

/// HINCRBY command handler
pub struct HIncrByCommand;

impl HIncrByCommand {
  /// Parse arguments from RESP items
  /// Format: HINCRBY key field increment
  fn parse_args(items: &[Value]) -> Result<HIncrByArgs, ProtocolError> {
    // HINCRBY key field increment (4 items)
    if items.len() != 4 {
      return Err(ProtocolError::WrongArgCount("hincrby"));
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

    // Parse increment
    let increment = match &items[3] {
      Value::BulkString(Some(data)) => {
        let s = String::from_utf8_lossy(data);
        s.parse::<i64>().map_err(|_| ProtocolError::NotAnInteger)?
      }
      Value::SimpleString(s) => s.parse::<i64>().map_err(|_| ProtocolError::NotAnInteger)?,
      Value::Integer(i) => *i,
      _ => return Err(ProtocolError::NotAnInteger),
    };

    Ok(HIncrByArgs {
      key,
      field,
      increment,
    })
  }

  /// Parse a byte array to i64
  fn parse_value_to_i64(data: &[u8]) -> Result<i64, ()> {
    let s = String::from_utf8_lossy(data);
    s.parse::<i64>().map_err(|_| ())
  }
}

#[async_trait]
impl Command for HIncrByCommand {
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

    // Build the sub-key for this field
    let sub_key_str = HashFieldValue::build_sub_key_hex(args.key.as_bytes(), version, &args.field);

    // Get current value
    let current_value: i64 = match server.get(&sub_key_str).await? {
      Some(raw_value) => match HashFieldValue::deserialize(&raw_value) {
        Ok(field_value) => {
          // Parse current value as i64
          match Self::parse_value_to_i64(&field_value.data) {
            Ok(v) => v,
            Err(_) => {
              return Err(ProtocolError::Custom("ERR hash value is not an integer").into());
            }
          }
        }
        Err(_) => {
          // Corrupted, treat as 0
          0
        }
      },
      None => {
        // Field doesn't exist, start with 0
        0
      }
    };

    // Perform increment with overflow check
    let new_value = current_value
      .checked_add(args.increment)
      .ok_or(ProtocolError::Overflow)?;

    // Check if field is new
    let field_exists = (server.get(&sub_key_str).await?).is_some();

    // Prepare batch write entries
    let mut entries: Vec<UpsertKV> = Vec::new();

    // Update field value
    let field_value = HashFieldValue::new(new_value.to_string());
    entries.push(UpsertKV::insert(sub_key_str, &field_value.serialize()));

    // Update metadata if this is a new field
    if !field_exists {
      metadata.incr_size();
    }

    // Add metadata entry
    entries.push(UpsertKV::insert(args.key.clone(), &metadata.serialize()));

    // Perform atomic batch write
    server.batch_write(entries).await?;

    // Return the new value
    Ok(Value::Integer(new_value))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_parse_args_basic() {
    // HINCRBY key field 5
    let items = vec![
      Value::SimpleString("HINCRBY".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"field1".to_vec())),
      Value::BulkString(Some(b"5".to_vec())),
    ];

    let args = HIncrByCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mykey");
    assert_eq!(args.field, b"field1");
    assert_eq!(args.increment, 5);
  }

  #[test]
  fn test_parse_args_negative() {
    // HINCRBY key field -10
    let items = vec![
      Value::SimpleString("HINCRBY".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"field1".to_vec())),
      Value::BulkString(Some(b"-10".to_vec())),
    ];

    let args = HIncrByCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mykey");
    assert_eq!(args.field, b"field1");
    assert_eq!(args.increment, -10);
  }

  #[test]
  fn test_parse_args_integer_type() {
    // HINCRBY with Integer type
    let items = vec![
      Value::SimpleString("HINCRBY".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"field1".to_vec())),
      Value::Integer(42),
    ];

    let args = HIncrByCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mykey");
    assert_eq!(args.field, b"field1");
    assert_eq!(args.increment, 42);
  }

  #[test]
  fn test_parse_args_insufficient_args() {
    // HINCRBY key field (missing increment)
    let items = vec![
      Value::SimpleString("HINCRBY".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"field1".to_vec())),
    ];

    let result = HIncrByCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_too_many_args() {
    // HINCRBY key field 5 extra
    let items = vec![
      Value::SimpleString("HINCRBY".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"field1".to_vec())),
      Value::BulkString(Some(b"5".to_vec())),
      Value::BulkString(Some(b"extra".to_vec())),
    ];

    let result = HIncrByCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_invalid_increment() {
    // HINCRBY key field not_a_number
    let items = vec![
      Value::SimpleString("HINCRBY".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"field1".to_vec())),
      Value::BulkString(Some(b"not_a_number".to_vec())),
    ];

    let result = HIncrByCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_large_number() {
    // HINCRBY key field 9223372036854775807 (max i64)
    let items = vec![
      Value::SimpleString("HINCRBY".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"field1".to_vec())),
      Value::BulkString(Some(b"9223372036854775807".to_vec())),
    ];

    let args = HIncrByCommand::parse_args(&items).unwrap();
    assert_eq!(args.increment, i64::MAX);
  }

  #[test]
  fn test_parse_args_min_number() {
    // HINCRBY key field -9223372036854775808 (min i64)
    let items = vec![
      Value::SimpleString("HINCRBY".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"field1".to_vec())),
      Value::BulkString(Some(b"-9223372036854775808".to_vec())),
    ];

    let args = HIncrByCommand::parse_args(&items).unwrap();
    assert_eq!(args.increment, i64::MIN);
  }

  #[test]
  fn test_parse_args_overflow_number() {
    // HINCRBY key field 9223372036854775808 (overflow)
    let items = vec![
      Value::SimpleString("HINCRBY".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"field1".to_vec())),
      Value::BulkString(Some(b"9223372036854775808".to_vec())),
    ];

    let result = HIncrByCommand::parse_args(&items);
    assert!(result.is_err());
  }
}
