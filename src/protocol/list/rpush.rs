//! RPUSH command implementation
//!
//! RPUSH key element [element ...]
//! Insert all the specified values at the tail of the list stored at key.
//! If key does not exist, it is created as empty list before performing the push.
//! When key holds a value that is not a list, an error is returned.
//!
//! Returns:
//! - The length of the list after the push operations (integer reply)
//!
//! Note: Elements are inserted one after the other from leftmost to rightmost.
//! `RPUSH mylist a b c` results in `[a, b, c]`.

use rockraft::raft::types::UpsertKV;

use crate::encoding::{ListElementValue, ListMetadata, TYPE_LIST};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

pub struct RPushCommand;

impl RPushCommand {
  fn parse_args(items: &[Value]) -> Result<RPushArgs, Value> {
    if items.len() < 3 {
      return Err(Value::error(
        "ERR wrong number of arguments for 'rpush' command",
      ));
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(Value::error("ERR invalid key")),
    };

    let mut elements = Vec::with_capacity(items.len() - 2);
    for item in &items[2..] {
      let elem = match item {
        Value::BulkString(Some(data)) => data.clone(),
        Value::SimpleString(s) => s.as_bytes().to_vec(),
        _ => return Err(Value::error("ERR invalid element")),
      };
      elements.push(elem);
    }

    Ok(RPushArgs { key, elements })
  }
}

struct RPushArgs {
  key: String,
  elements: Vec<Vec<u8>>,
}

#[async_trait]
impl Command for RPushCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Value {
    let args = match Self::parse_args(items) {
      Ok(args) => args,
      Err(err) => return err,
    };

    let mut metadata = match server.get(&args.key).await {
      Ok(Some(raw_meta)) => match ListMetadata::deserialize(&raw_meta) {
        Ok(meta) => {
          if meta.get_type() != TYPE_LIST {
            return Value::error(
              "WRONGTYPE Operation against a key holding the wrong kind of value",
            );
          }
          if meta.is_expired(now_ms()) {
            ListMetadata::new()
          } else {
            meta
          }
        }
        Err(_) => ListMetadata::new(),
      },
      _ => ListMetadata::new(),
    };

    let version = metadata.version;
    let tail = metadata.tail;

    let mut entries: Vec<UpsertKV> = Vec::with_capacity(args.elements.len() + 1);

    // RPUSH mylist a b c → a goes to tail, b goes to tail+1, c goes to tail+2
    // Final list order: [a, b, c]
    for (i, elem_data) in args.elements.iter().enumerate() {
      let index = tail + i as u64;
      let sub_key_str = ListElementValue::build_sub_key_hex(args.key.as_bytes(), version, index);

      let elem_value = ListElementValue::new(elem_data.clone());
      entries.push(UpsertKV::insert(sub_key_str, &elem_value.serialize()));
    }

    metadata.tail += args.elements.len() as u64;
    metadata.size += args.elements.len() as u64;

    entries.push(UpsertKV::insert(args.key.clone(), &metadata.serialize()));

    if let Err(e) = server.batch_write(entries).await {
      return Value::error(format!("ERR failed to batch write: {}", e));
    }

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
    let items = vec![
      Value::SimpleString("RPUSH".to_string()),
      bulk(b"mylist"),
      bulk(b"hello"),
    ];

    let args = RPushCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mylist");
    assert_eq!(args.elements.len(), 1);
    assert_eq!(args.elements[0], b"hello");
  }

  #[test]
  fn test_parse_args_multiple_elements() {
    let items = vec![
      Value::SimpleString("RPUSH".to_string()),
      bulk(b"mylist"),
      bulk(b"a"),
      bulk(b"b"),
      bulk(b"c"),
    ];

    let args = RPushCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mylist");
    assert_eq!(args.elements.len(), 3);
    assert_eq!(args.elements[0], b"a");
    assert_eq!(args.elements[1], b"b");
    assert_eq!(args.elements[2], b"c");
  }

  #[test]
  fn test_parse_args_insufficient() {
    let items = vec![Value::SimpleString("RPUSH".to_string()), bulk(b"mylist")];
    let result = RPushCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_no_args() {
    let items = vec![Value::SimpleString("RPUSH".to_string())];
    let result = RPushCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_simple_string_key() {
    let items = vec![
      Value::SimpleString("RPUSH".to_string()),
      Value::SimpleString("mylist".to_string()),
      Value::SimpleString("value".to_string()),
    ];

    let args = RPushCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mylist");
    assert_eq!(args.elements.len(), 1);
    assert_eq!(args.elements[0], b"value");
  }

  #[test]
  fn test_parse_args_binary_element() {
    let items = vec![
      Value::SimpleString("RPUSH".to_string()),
      bulk(b"mylist"),
      bulk(b"\x00\x01\xff"),
    ];

    let args = RPushCommand::parse_args(&items).unwrap();
    assert_eq!(args.elements[0], b"\x00\x01\xff");
  }

  #[test]
  fn test_parse_args_empty_element() {
    let items = vec![
      Value::SimpleString("RPUSH".to_string()),
      bulk(b"mylist"),
      bulk(b""),
    ];

    let args = RPushCommand::parse_args(&items).unwrap();
    assert_eq!(args.elements.len(), 1);
    assert!(args.elements[0].is_empty());
  }

  #[test]
  fn test_parse_args_invalid_key_type() {
    let items = vec![
      Value::SimpleString("RPUSH".to_string()),
      Value::Integer(42),
      bulk(b"element"),
    ];
    let result = RPushCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_invalid_element_type() {
    let items = vec![
      Value::SimpleString("RPUSH".to_string()),
      bulk(b"mylist"),
      Value::Integer(42),
    ];
    let result = RPushCommand::parse_args(&items);
    assert!(result.is_err());
  }
}
