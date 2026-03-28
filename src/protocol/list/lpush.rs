//! LPUSH command implementation
//!
//! LPUSH key element [element ...]
//! Insert all the specified values at the head of the list stored at key.
//! If key does not exist, it is created as empty list before performing the push.
//! When key holds a value that is not a list, an error is returned.
//!
//! Returns:
//! - The length of the list after the push operations (integer reply)
//!
//! Note: Elements are inserted one after the other from leftmost to rightmost.
//! `LPUSH mylist a b c` results in `[c, b, a]`.

use rockraft::raft::types::UpsertKV;

use crate::encoding::{ListElementValue, ListMetadata, TYPE_LIST};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

/// LPUSH command handler
pub struct LPushCommand;

impl LPushCommand {
  /// Parse arguments from RESP items
  /// Format: LPUSH key element [element ...]
  fn parse_args(items: &[Value]) -> Result<LPushArgs, Value> {
    // Minimum: LPUSH key element (3 items)
    if items.len() < 3 {
      return Err(Value::error(
        "ERR wrong number of arguments for 'lpush' command",
      ));
    }

    // Parse key
    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(Value::error("ERR invalid key")),
    };

    // Parse elements from items[2..]
    let mut elements = Vec::with_capacity(items.len() - 2);
    for item in &items[2..] {
      let elem = match item {
        Value::BulkString(Some(data)) => data.clone(),
        Value::SimpleString(s) => s.as_bytes().to_vec(),
        _ => return Err(Value::error("ERR invalid element")),
      };
      elements.push(elem);
    }

    Ok(LPushArgs { key, elements })
  }
}

/// Parsed LPUSH arguments
struct LPushArgs {
  key: String,
  elements: Vec<Vec<u8>>,
}

#[async_trait]
impl Command for LPushCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Value {
    // Parse arguments
    let args = match Self::parse_args(items) {
      Ok(args) => args,
      Err(err) => return err,
    };

    // Get or create metadata
    let mut metadata = match server.get(&args.key).await {
      Ok(Some(raw_meta)) => match ListMetadata::deserialize(&raw_meta) {
        Ok(meta) => {
          // Check if it's actually a list type
          if meta.get_type() != TYPE_LIST {
            return Value::error(
              "WRONGTYPE Operation against a key holding the wrong kind of value",
            );
          }
          // Check if expired
          if meta.is_expired(now_ms()) {
            ListMetadata::new()
          } else {
            meta
          }
        }
        Err(_) => {
          // Corrupted, create new
          ListMetadata::new()
        }
      },
      _ => {
        // Not found, create new
        ListMetadata::new()
      }
    };

    let version = metadata.version;
    let head = metadata.head;

    // Prepare batch write entries
    let mut entries: Vec<UpsertKV> = Vec::with_capacity(args.elements.len() + 1);

    // Insert elements at the head, from leftmost to rightmost.
    // LPUSH mylist a b c → a goes to head-1, b goes to head-2, c goes to head-3
    // Final list order: [c, b, a] (c at position 0)
    for (i, elem_data) in args.elements.iter().enumerate() {
      let index = head - 1 - i as u64;
      let sub_key_str = ListElementValue::build_sub_key_hex(args.key.as_bytes(), version, index);

      let elem_value = ListElementValue::new(elem_data.clone());
      entries.push(UpsertKV::insert(sub_key_str, &elem_value.serialize()));
    }

    // Update metadata: decrement head, increment size
    metadata.head -= args.elements.len() as u64;
    metadata.size += args.elements.len() as u64;

    // Add metadata entry
    entries.push(UpsertKV::insert(args.key.clone(), &metadata.serialize()));

    // Perform atomic batch write
    if let Err(e) = server.batch_write(entries).await {
      return Value::error(format!("ERR failed to batch write: {}", e));
    }

    // Return the new length of the list
    Value::Integer(metadata.size as i64)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn bulk(data: &[u8]) -> Value {
    Value::BulkString(Some(data.to_vec()))
  }

  #[test]
  fn test_parse_args_basic() {
    // LPUSH mylist element
    let items = vec![
      Value::SimpleString("LPUSH".to_string()),
      bulk(b"mylist"),
      bulk(b"hello"),
    ];

    let args = LPushCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mylist");
    assert_eq!(args.elements.len(), 1);
    assert_eq!(args.elements[0], b"hello");
  }

  #[test]
  fn test_parse_args_multiple_elements() {
    // LPUSH mylist a b c
    let items = vec![
      Value::SimpleString("LPUSH".to_string()),
      bulk(b"mylist"),
      bulk(b"a"),
      bulk(b"b"),
      bulk(b"c"),
    ];

    let args = LPushCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mylist");
    assert_eq!(args.elements.len(), 3);
    assert_eq!(args.elements[0], b"a");
    assert_eq!(args.elements[1], b"b");
    assert_eq!(args.elements[2], b"c");
  }

  #[test]
  fn test_parse_args_insufficient() {
    // LPUSH mylist (missing element)
    let items = vec![Value::SimpleString("LPUSH".to_string()), bulk(b"mylist")];

    let result = LPushCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_no_args() {
    // LPUSH
    let items = vec![Value::SimpleString("LPUSH".to_string())];
    let result = LPushCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_simple_string_key() {
    let items = vec![
      Value::SimpleString("LPUSH".to_string()),
      Value::SimpleString("mylist".to_string()),
      Value::SimpleString("value".to_string()),
    ];

    let args = LPushCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mylist");
    assert_eq!(args.elements.len(), 1);
    assert_eq!(args.elements[0], b"value");
  }

  #[test]
  fn test_parse_args_binary_element() {
    let items = vec![
      Value::SimpleString("LPUSH".to_string()),
      bulk(b"mylist"),
      bulk(b"\x00\x01\xff"),
    ];

    let args = LPushCommand::parse_args(&items).unwrap();
    assert_eq!(args.elements[0], b"\x00\x01\xff");
  }

  #[test]
  fn test_parse_args_empty_element() {
    let items = vec![
      Value::SimpleString("LPUSH".to_string()),
      bulk(b"mylist"),
      bulk(b""),
    ];

    let args = LPushCommand::parse_args(&items).unwrap();
    assert_eq!(args.elements.len(), 1);
    assert!(args.elements[0].is_empty());
  }

  #[test]
  fn test_parse_args_invalid_key_type() {
    let items = vec![
      Value::SimpleString("LPUSH".to_string()),
      Value::Integer(42),
      bulk(b"element"),
    ];

    let result = LPushCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_invalid_element_type() {
    let items = vec![
      Value::SimpleString("LPUSH".to_string()),
      bulk(b"mylist"),
      Value::Integer(42),
    ];

    let result = LPushCommand::parse_args(&items);
    assert!(result.is_err());
  }
}
