//! HSET command implementation
//!
//! HSET key field value [field value ...]
//! Sets the specified fields to their respective values in the hash stored at key.
//!
//! Returns:
//! - The number of fields that were added (not updated)
//!
//! Note: This command uses atomic batch write to ensure all fields and metadata
//! are written together as a single atomic operation.

use rockraft::raft::types::UpsertKV;

use crate::encoding::{HashFieldValue, HashMetadata};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

/// Parsed HSET arguments
#[derive(Debug)]
struct HSetArgs {
  key: String,
  fields: Vec<(Vec<u8>, Vec<u8>)>, // (field, value) pairs
}

/// HSET command handler
pub struct HSetCommand;

impl HSetCommand {
  /// Parse arguments from RESP items
  /// Format: HSET key field value [field value ...]
  fn parse_args(items: &[Value]) -> Result<HSetArgs, Value> {
    // Minimum: HSET key field value (4 items)
    if items.len() < 4 {
      return Err(Value::error(
        "ERR wrong number of arguments for 'hset' command",
      ));
    }

    // Parse key
    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(Value::error("ERR invalid key")),
    };

    // Parse field-value pairs from items[2..]
    let field_count = items.len() - 2; // items[2] onwards
    if !field_count.is_multiple_of(2) {
      return Err(Value::error(
        "ERR wrong number of arguments for 'hset' command",
      ));
    }

    let mut fields = Vec::with_capacity(field_count / 2);
    let mut i = 2;
    while i < items.len() {
      let field = match &items[i] {
        Value::BulkString(Some(data)) => data.clone(),
        Value::SimpleString(s) => s.as_bytes().to_vec(),
        _ => return Err(Value::error("ERR invalid field")),
      };

      let value = match &items[i + 1] {
        Value::BulkString(Some(data)) => data.clone(),
        Value::SimpleString(s) => s.as_bytes().to_vec(),
        _ => return Err(Value::error("ERR invalid value")),
      };

      fields.push((field, value));
      i += 2;
    }

    Ok(HSetArgs { key, fields })
  }
}

#[async_trait]
impl Command for HSetCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Value {
    // Parse arguments
    let args = match Self::parse_args(items) {
      Ok(args) => args,
      Err(err) => return err,
    };

    // Get or create metadata
    let mut metadata = match server.get(&args.key).await {
      Ok(Some(raw_meta)) => match HashMetadata::deserialize(&raw_meta) {
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
      _ => {
        // Not found, create new
        HashMetadata::new()
      }
    };

    let version = metadata.version;
    let mut added_count = 0i64;

    // Prepare batch write entries
    let mut entries: Vec<UpsertKV> = Vec::new();

    // Process each field-value pair and prepare entries
    for (field, value_data) in &args.fields {
      // Use hex-encoded sub_key for storage (guaranteed valid UTF-8)
      let sub_key_str = HashFieldValue::build_sub_key_hex(args.key.as_bytes(), version, field);

      // Check if field exists (for counting added fields)
      let field_exists = matches!(server.get(&sub_key_str).await, Ok(Some(_)));

      // Prepare field value entry
      let field_value = HashFieldValue::new(value_data.clone());
      entries.push(UpsertKV::insert(sub_key_str, &field_value.serialize()));

      // Update metadata if this is a new field
      if !field_exists {
        metadata.incr_size();
        added_count += 1;
      }
    }

    // Add metadata entry
    entries.push(UpsertKV::insert(args.key.clone(), &metadata.serialize()));

    // Perform atomic batch write
    if let Err(e) = server.batch_write(entries).await {
      return Value::error(format!("ERR failed to batch write: {}", e));
    }

    // Return the number of newly added fields
    Value::Integer(added_count)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_parse_args_basic() {
    // HSET key field value
    let items = vec![
      Value::SimpleString("HSET".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"field1".to_vec())),
      Value::BulkString(Some(b"value1".to_vec())),
    ];

    let args = HSetCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mykey");
    assert_eq!(args.fields.len(), 1);
    assert_eq!(args.fields[0].0, b"field1");
    assert_eq!(args.fields[0].1, b"value1");
  }

  #[test]
  fn test_parse_args_multiple_fields() {
    // HSET key f1 v1 f2 v2 f3 v3
    let items = vec![
      Value::SimpleString("HSET".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"f1".to_vec())),
      Value::BulkString(Some(b"v1".to_vec())),
      Value::BulkString(Some(b"f2".to_vec())),
      Value::BulkString(Some(b"v2".to_vec())),
      Value::BulkString(Some(b"f3".to_vec())),
      Value::BulkString(Some(b"v3".to_vec())),
    ];

    let args = HSetCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mykey");
    assert_eq!(args.fields.len(), 3);
    assert_eq!(args.fields[0].0, b"f1");
    assert_eq!(args.fields[0].1, b"v1");
    assert_eq!(args.fields[1].0, b"f2");
    assert_eq!(args.fields[1].1, b"v2");
    assert_eq!(args.fields[2].0, b"f3");
    assert_eq!(args.fields[2].1, b"v3");
  }

  #[test]
  fn test_parse_args_insufficient_args() {
    // HSET key (missing field and value)
    let items = vec![
      Value::SimpleString("HSET".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
    ];

    let result = HSetCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_odd_field_count() {
    // HSET key f1 v1 f2 (missing v2)
    let items = vec![
      Value::SimpleString("HSET".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"f1".to_vec())),
      Value::BulkString(Some(b"v1".to_vec())),
      Value::BulkString(Some(b"f2".to_vec())),
    ];

    let result = HSetCommand::parse_args(&items);
    assert!(result.is_err());
  }
}
