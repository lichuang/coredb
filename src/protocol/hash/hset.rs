//! HSET command implementation
//!
//! HSET key field value [field value ...] [NX | XX]
//! Sets field in the hash stored at key to value.
//!
//! Options:
//! - NX: Only set the field if it does not already exist
//! - XX: Only set the field if it already exists
//!
//! Returns:
//! - Without NX/XX: the number of fields that were added (not updated)
//! - With NX/XX: 1 if the field was set, 0 if not (due to condition)

use crate::encoding::{HashFieldValue, HashMetadata};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

/// HSET command options
#[derive(Debug, Clone, Copy, PartialEq)]
enum HSetOption {
  /// No special option
  None,
  /// Only set if field does not exist
  NX,
  /// Only set if field exists
  XX,
}

/// Parsed HSET arguments
#[derive(Debug)]
struct HSetArgs {
  key: String,
  fields: Vec<(Vec<u8>, Vec<u8>)>, // (field, value) pairs
  option: HSetOption,
}

/// HSET command handler
pub struct HSetCommand;

impl HSetCommand {
  /// Parse arguments from RESP items
  /// Format: HSET key field value [field value ...] [NX | XX]
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

    // Check for NX/XX option (must be at the end)
    let mut end_idx = items.len();
    let mut option = HSetOption::None;

    // Check if last item is NX or XX
    #[allow(clippy::collapsible_if)]
    if items.len() >= 5 {
      if let Some(opt_str) = Self::extract_string(&items[items.len() - 1]) {
        match opt_str.to_uppercase().as_str() {
          "NX" => {
            option = HSetOption::NX;
            end_idx = items.len() - 1;
          }
          "XX" => {
            option = HSetOption::XX;
            end_idx = items.len() - 1;
          }
          _ => {}
        }
      }
    }

    // Parse field-value pairs from items[2..end_idx]
    let field_count = end_idx - 2; // items[2] onwards
    if !field_count.is_multiple_of(2) {
      return Err(Value::error(
        "ERR wrong number of arguments for 'hset' command",
      ));
    }

    let mut fields = Vec::with_capacity(field_count / 2);
    let mut i = 2;
    while i < end_idx {
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

    Ok(HSetArgs {
      key,
      fields,
      option,
    })
  }

  /// Extract string from Value
  fn extract_string(value: &Value) -> Option<String> {
    match value {
      Value::BulkString(Some(data)) => Some(String::from_utf8_lossy(data).to_string()),
      Value::SimpleString(s) => Some(s.clone()),
      _ => None,
    }
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
    let mut set_count = 0i64; // Count fields actually set (for NX/XX return value)

    // Process each field-value pair
    for (field, value_data) in args.fields {
      // Use hex-encoded sub_key for storage (guaranteed valid UTF-8)
      let sub_key_str = HashFieldValue::build_sub_key_hex(args.key.as_bytes(), version, &field);

      // Check if field exists
      let field_exists = matches!(server.get(&sub_key_str).await, Ok(Some(_)));

      // Apply NX/XX conditions
      match args.option {
        HSetOption::NX => {
          // Only set if field does NOT exist
          if field_exists {
            continue;
          }
        }
        HSetOption::XX => {
          // Only set if field DOES exist
          if !field_exists {
            continue;
          }
        }
        HSetOption::None => {
          // No condition, always set
        }
      }

      // Store the field value
      let field_value = HashFieldValue::new(value_data);
      if let Err(e) = server.set(sub_key_str, field_value.serialize()).await {
        return Value::error(format!("ERR failed to store field: {}", e));
      }

      set_count += 1;

      // Update metadata if this is a new field
      if !field_exists {
        metadata.incr_size();
        added_count += 1;
      }
    }

    // Store metadata (only if we modified something or it's a new hash)
    if let Err(e) = server.set(args.key.clone(), metadata.serialize()).await {
      return Value::error(format!("ERR failed to store metadata: {}", e));
    }

    // Return value depends on options:
    // - With NX/XX: 1 if at least one field was set, 0 otherwise
    // - Without options: number of newly added fields
    match args.option {
      HSetOption::NX | HSetOption::XX => {
        // For NX/XX, return 1 if any field was set
        Value::Integer(if set_count > 0 { 1 } else { 0 })
      }
      HSetOption::None => {
        // Without options, return number of newly added fields
        Value::Integer(added_count)
      }
    }
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
    assert_eq!(args.option, HSetOption::None);
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
    assert_eq!(args.option, HSetOption::None);
  }

  #[test]
  fn test_parse_args_with_nx() {
    // HSET key field value NX
    let items = vec![
      Value::SimpleString("HSET".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"field1".to_vec())),
      Value::BulkString(Some(b"value1".to_vec())),
      Value::BulkString(Some(b"NX".to_vec())),
    ];

    let args = HSetCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mykey");
    assert_eq!(args.fields.len(), 1);
    assert_eq!(args.option, HSetOption::NX);
  }

  #[test]
  fn test_parse_args_with_xx() {
    // HSET key field value XX
    let items = vec![
      Value::SimpleString("HSET".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"field1".to_vec())),
      Value::BulkString(Some(b"value1".to_vec())),
      Value::BulkString(Some(b"XX".to_vec())),
    ];

    let args = HSetCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mykey");
    assert_eq!(args.fields.len(), 1);
    assert_eq!(args.option, HSetOption::XX);
  }

  #[test]
  fn test_parse_args_multiple_fields_with_nx() {
    // HSET key f1 v1 f2 v2 NX
    let items = vec![
      Value::SimpleString("HSET".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"f1".to_vec())),
      Value::BulkString(Some(b"v1".to_vec())),
      Value::BulkString(Some(b"f2".to_vec())),
      Value::BulkString(Some(b"v2".to_vec())),
      Value::BulkString(Some(b"NX".to_vec())),
    ];

    let args = HSetCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mykey");
    assert_eq!(args.fields.len(), 2);
    assert_eq!(args.option, HSetOption::NX);
  }

  #[test]
  fn test_parse_args_case_insensitive() {
    // HSET key field value nx (lowercase)
    let items = vec![
      Value::SimpleString("HSET".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"field1".to_vec())),
      Value::BulkString(Some(b"value1".to_vec())),
      Value::BulkString(Some(b"nx".to_vec())),
    ];

    let args = HSetCommand::parse_args(&items).unwrap();
    assert_eq!(args.option, HSetOption::NX);
  }

  #[test]
  fn test_parse_args_not_option_if_not_last() {
    // HSET key NX value - "NX" here is a field name, not an option
    let items = vec![
      Value::SimpleString("HSET".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"NX".to_vec())),
      Value::BulkString(Some(b"value1".to_vec())),
    ];

    let args = HSetCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mykey");
    assert_eq!(args.fields.len(), 1);
    assert_eq!(args.fields[0].0, b"NX");
    assert_eq!(args.fields[0].1, b"value1");
    assert_eq!(args.option, HSetOption::None);
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

  #[test]
  fn test_parse_args_nx_in_middle_not_option() {
    // HSET key f1 v1 NX v2 - "NX" is field2, "v2" is value2
    let items = vec![
      Value::SimpleString("HSET".to_string()),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"f1".to_vec())),
      Value::BulkString(Some(b"v1".to_vec())),
      Value::BulkString(Some(b"NX".to_vec())),
      Value::BulkString(Some(b"v2".to_vec())),
    ];

    let args = HSetCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mykey");
    assert_eq!(args.fields.len(), 2);
    assert_eq!(args.fields[0].0, b"f1");
    assert_eq!(args.fields[0].1, b"v1");
    assert_eq!(args.fields[1].0, b"NX");
    assert_eq!(args.fields[1].1, b"v2");
    assert_eq!(args.option, HSetOption::None);
  }
}
