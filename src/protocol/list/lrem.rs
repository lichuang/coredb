//! LREM command implementation
//!
//! LREM key count element
//! Removes the first `count` occurrences of elements equal to `element` from the list.
//!
//! - `count > 0`: Remove elements moving from head to tail.
//! - `count < 0`: Remove elements moving from tail to head.
//! - `count = 0`: Remove all elements equal to `element`.
//!
//! Return value: Integer reply — the number of removed elements.
//! If the key does not exist, returns 0.
//! If the list becomes empty after removal, the key is deleted.

use rockraft::raft::types::UpsertKV;

use crate::encoding::{ListElementValue, ListMetadata, TYPE_LIST};
use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

pub struct LRemCommand;

struct LRemArgs {
  key: String,
  count: i64,
  element: Vec<u8>,
}

impl LRemCommand {
  fn parse_args(items: &[Value]) -> Result<LRemArgs, ProtocolError> {
    if items.len() != 4 {
      return Err(ProtocolError::WrongArgCount("lrem"));
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::InvalidArgument("key")),
    };

    let count = parse_i64(&items[2])?;

    let element = match &items[3] {
      Value::BulkString(Some(data)) => data.clone(),
      Value::BulkString(None) => Vec::new(),
      Value::SimpleString(s) => s.as_bytes().to_vec(),
      _ => return Err(ProtocolError::InvalidArgument("element")),
    };

    Ok(LRemArgs {
      key,
      count,
      element,
    })
  }
}

fn parse_i64(value: &Value) -> Result<i64, ProtocolError> {
  match value {
    Value::BulkString(Some(data)) => {
      let s = String::from_utf8_lossy(data);
      s.parse::<i64>().map_err(|_| ProtocolError::NotAnInteger)
    }
    Value::SimpleString(s) => s.parse::<i64>().map_err(|_| ProtocolError::NotAnInteger),
    Value::Integer(n) => Ok(*n),
    _ => Err(ProtocolError::NotAnInteger),
  }
}

#[async_trait]
impl Command for LRemCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    let args = Self::parse_args(items)?;

    let metadata = match server.get(&args.key).await? {
      Some(raw_meta) => match ListMetadata::deserialize(&raw_meta) {
        Ok(meta) => {
          if meta.get_type() != TYPE_LIST {
            return Err(ProtocolError::WrongType.into());
          }
          if meta.is_expired(now_ms()) {
            return Ok(Value::Integer(0));
          }
          meta
        }
        Err(_) => return Ok(Value::Integer(0)),
      },
      None => return Ok(Value::Integer(0)),
    };

    if metadata.size == 0 {
      return Ok(Value::Integer(0));
    }

    let version = metadata.version;
    let target = &args.element;

    let mut all_elements: Vec<(u64, Vec<u8>)> = Vec::with_capacity(metadata.size as usize);
    for i in 0..metadata.size {
      let physical = metadata.head + i;
      let sub_key = ListElementValue::build_sub_key_hex(args.key.as_bytes(), version, physical);
      match server.get(&sub_key).await? {
        Some(raw_elem) => match ListElementValue::deserialize(&raw_elem) {
          Ok(elem) => all_elements.push((physical, elem.data)),
          Err(_) => break,
        },
        None => break,
      }
    }

    // Determine which indices to remove based on count sign
    let indices_to_remove = if args.count > 0 {
      let mut to_remove = Vec::new();
      let mut remaining = args.count as usize;
      for (physical, data) in &all_elements {
        if remaining == 0 {
          break;
        }
        if data == target {
          to_remove.push(*physical);
          remaining -= 1;
        }
      }
      to_remove
    } else if args.count < 0 {
      let mut to_remove = Vec::new();
      let mut remaining = (-args.count) as usize;
      for (physical, data) in all_elements.iter().rev() {
        if remaining == 0 {
          break;
        }
        if data == target {
          to_remove.push(*physical);
          remaining -= 1;
        }
      }
      to_remove
    } else {
      all_elements
        .iter()
        .filter(|(_, data)| data == target)
        .map(|(physical, _)| *physical)
        .collect()
    };

    let removed_count = indices_to_remove.len() as u64;
    if removed_count == 0 {
      return Ok(Value::Integer(0));
    }

    let new_size = metadata.size - removed_count;

    let mut entries: Vec<UpsertKV> = Vec::with_capacity(indices_to_remove.len() + 1);

    for physical in &indices_to_remove {
      let sub_key = ListElementValue::build_sub_key_hex(args.key.as_bytes(), version, *physical);
      entries.push(UpsertKV::delete(sub_key));
    }

    if new_size == 0 {
      entries.push(UpsertKV::delete(args.key.clone()));
    } else {
      let mut new_meta = metadata.clone();
      new_meta.size = new_size;
      entries.push(UpsertKV::insert(args.key.clone(), &new_meta.serialize()));
    }

    server.batch_write(entries).await?;

    Ok(Value::Integer(removed_count as i64))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn bulk(data: &[u8]) -> Value {
    Value::BulkString(Some(data.to_vec()))
  }

  #[test]
  fn test_parse_args_valid_positive() {
    let items = vec![
      Value::SimpleString("LREM".to_string()),
      bulk(b"mylist"),
      Value::Integer(2),
      bulk(b"hello"),
    ];
    let args = LRemCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mylist");
    assert_eq!(args.count, 2);
    assert_eq!(args.element, b"hello");
  }

  #[test]
  fn test_parse_args_valid_negative() {
    let items = vec![
      Value::SimpleString("LREM".to_string()),
      bulk(b"mylist"),
      Value::Integer(-3),
      bulk(b"world"),
    ];
    let args = LRemCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mylist");
    assert_eq!(args.count, -3);
    assert_eq!(args.element, b"world");
  }

  #[test]
  fn test_parse_args_valid_zero() {
    let items = vec![
      Value::SimpleString("LREM".to_string()),
      bulk(b"mylist"),
      Value::Integer(0),
      bulk(b"elem"),
    ];
    let args = LRemCommand::parse_args(&items).unwrap();
    assert_eq!(args.count, 0);
  }

  #[test]
  fn test_parse_args_string_count() {
    let items = vec![
      Value::SimpleString("LREM".to_string()),
      bulk(b"mylist"),
      bulk(b"5"),
      bulk(b"value"),
    ];
    let args = LRemCommand::parse_args(&items).unwrap();
    assert_eq!(args.count, 5);
  }

  #[test]
  fn test_parse_args_too_few() {
    let items = vec![
      Value::SimpleString("LREM".to_string()),
      bulk(b"mylist"),
      Value::Integer(2),
    ];
    assert!(LRemCommand::parse_args(&items).is_err());
  }

  #[test]
  fn test_parse_args_too_many() {
    let items = vec![
      Value::SimpleString("LREM".to_string()),
      bulk(b"mylist"),
      Value::Integer(2),
      bulk(b"hello"),
      bulk(b"extra"),
    ];
    assert!(LRemCommand::parse_args(&items).is_err());
  }

  #[test]
  fn test_parse_args_no_key() {
    let items = vec![Value::SimpleString("LREM".to_string())];
    assert!(LRemCommand::parse_args(&items).is_err());
  }

  #[test]
  fn test_parse_args_invalid_count() {
    let items = vec![
      Value::SimpleString("LREM".to_string()),
      bulk(b"mylist"),
      bulk(b"abc"),
      bulk(b"value"),
    ];
    assert!(LRemCommand::parse_args(&items).is_err());
  }

  #[test]
  fn test_parse_args_empty_key() {
    let items = vec![
      Value::SimpleString("LREM".to_string()),
      bulk(b""),
      Value::Integer(1),
      bulk(b"value"),
    ];
    let args = LRemCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "");
  }

  #[test]
  fn test_parse_args_empty_element() {
    let items = vec![
      Value::SimpleString("LREM".to_string()),
      bulk(b"mylist"),
      Value::Integer(1),
      bulk(b""),
    ];
    let args = LRemCommand::parse_args(&items).unwrap();
    assert_eq!(args.element, b"");
  }

  #[test]
  fn test_parse_args_nil_element() {
    let items = vec![
      Value::SimpleString("LREM".to_string()),
      bulk(b"mylist"),
      Value::Integer(1),
      Value::BulkString(None),
    ];
    let args = LRemCommand::parse_args(&items).unwrap();
    assert_eq!(args.element, b"");
  }

  #[test]
  fn test_parse_args_simplestring_element() {
    let items = vec![
      Value::SimpleString("LREM".to_string()),
      bulk(b"mylist"),
      Value::Integer(1),
      Value::SimpleString("hello".to_string()),
    ];
    let args = LRemCommand::parse_args(&items).unwrap();
    assert_eq!(args.element, b"hello");
  }

  #[test]
  fn test_parse_args_binary_element() {
    let binary_data: Vec<u8> = (0..=255).collect();
    let items = vec![
      Value::SimpleString("LREM".to_string()),
      bulk(b"mylist"),
      Value::Integer(1),
      Value::BulkString(Some(binary_data.clone())),
    ];
    let args = LRemCommand::parse_args(&items).unwrap();
    assert_eq!(args.element, binary_data);
  }

  #[test]
  fn test_parse_args_invalid_key_type() {
    let items = vec![
      Value::SimpleString("LREM".to_string()),
      Value::Integer(42),
      Value::Integer(1),
      bulk(b"value"),
    ];
    assert!(LRemCommand::parse_args(&items).is_err());
  }

  #[test]
  fn test_parse_args_invalid_element_type() {
    let items = vec![
      Value::SimpleString("LREM".to_string()),
      bulk(b"mylist"),
      Value::Integer(1),
      Value::Integer(42),
    ];
    assert!(LRemCommand::parse_args(&items).is_err());
  }
}
