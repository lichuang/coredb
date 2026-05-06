//! LSET command implementation
//!
//! LSET key index element
//! Sets the list element at index to element.
//
// The index is zero-based, so 0 means the first element, 1 the second, etc.
// Negative indices can be used to designate elements starting at the tail:
// -1 is the last element, -2 the second to last, etc.
//
// Return value:
// - SimpleString "OK" on success
// - Error: if key does not exist ("ERR no such key")
// - Error: if index is out of range ("ERR index out of range")
// - Error: if key exists but is not a list (WRONGTYPE)

use crate::encoding::{ListElementValue, ListMetadata, TYPE_LIST};
use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

pub struct LSetCommand;

struct LSetArgs {
  key: String,
  index: i64,
  element: Vec<u8>,
}

impl LSetCommand {
  fn parse_args(items: &[Value]) -> Result<LSetArgs, ProtocolError> {
    if items.len() != 4 {
      return Err(ProtocolError::WrongArgCount("lset"));
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::InvalidArgument("key")),
    };

    let index = parse_i64(&items[2])?;

    let element = match &items[3] {
      Value::BulkString(Some(data)) => data.clone(),
      Value::BulkString(None) => Vec::new(),
      Value::SimpleString(s) => s.as_bytes().to_vec(),
      _ => return Err(ProtocolError::InvalidArgument("element")),
    };

    Ok(LSetArgs {
      key,
      index,
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
impl Command for LSetCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    let args = Self::parse_args(items)?;

    // Get metadata
    let metadata = match server.get(&args.key).await? {
      Some(raw_meta) => match ListMetadata::deserialize(&raw_meta) {
        Ok(meta) => {
          if meta.get_type() != TYPE_LIST {
            return Err(ProtocolError::WrongType.into());
          }
          if meta.is_expired(now_ms()) {
            return Err(ProtocolError::Custom("ERR no such key").into());
          }
          meta
        }
        Err(_) => {
          return Err(ProtocolError::Custom("ERR no such key").into());
        }
      },
      None => {
        return Err(ProtocolError::Custom("ERR no such key").into());
      }
    };

    // Resolve the logical index to a physical index
    let physical = match metadata.resolve_index(args.index) {
      Some(p) => p,
      None => {
        return Err(ProtocolError::Custom("ERR index out of range").into());
      }
    };

    // Build sub-key and write the new element
    let sub_key =
      ListElementValue::build_sub_key_hex(args.key.as_bytes(), metadata.version, physical);
    let element = ListElementValue::new(args.element);

    server.set(sub_key, element.serialize()).await?;

    Ok(Value::ok())
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
      Value::SimpleString("LSET".to_string()),
      bulk(b"mylist"),
      Value::Integer(2),
      bulk(b"newvalue"),
    ];
    let args = LSetCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mylist");
    assert_eq!(args.index, 2);
    assert_eq!(args.element, b"newvalue");
  }

  #[test]
  fn test_parse_args_valid_negative() {
    let items = vec![
      Value::SimpleString("LSET".to_string()),
      bulk(b"mylist"),
      Value::Integer(-1),
      bulk(b"last"),
    ];
    let args = LSetCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mylist");
    assert_eq!(args.index, -1);
    assert_eq!(args.element, b"last");
  }

  #[test]
  fn test_parse_args_string_index() {
    let items = vec![
      Value::SimpleString("LSET".to_string()),
      bulk(b"mylist"),
      bulk(b"3"),
      bulk(b"value"),
    ];
    let args = LSetCommand::parse_args(&items).unwrap();
    assert_eq!(args.index, 3);
  }

  #[test]
  fn test_parse_args_too_few() {
    let items = vec![
      Value::SimpleString("LSET".to_string()),
      bulk(b"mylist"),
      Value::Integer(0),
    ];
    assert!(LSetCommand::parse_args(&items).is_err());
  }

  #[test]
  fn test_parse_args_too_many() {
    let items = vec![
      Value::SimpleString("LSET".to_string()),
      bulk(b"mylist"),
      Value::Integer(0),
      bulk(b"value"),
      bulk(b"extra"),
    ];
    assert!(LSetCommand::parse_args(&items).is_err());
  }

  #[test]
  fn test_parse_args_no_args() {
    let items = vec![Value::SimpleString("LSET".to_string())];
    assert!(LSetCommand::parse_args(&items).is_err());
  }

  #[test]
  fn test_parse_args_invalid_index() {
    let items = vec![
      Value::SimpleString("LSET".to_string()),
      bulk(b"mylist"),
      bulk(b"abc"),
      bulk(b"value"),
    ];
    assert!(LSetCommand::parse_args(&items).is_err());
  }

  #[test]
  fn test_parse_args_empty_key() {
    let items = vec![
      Value::SimpleString("LSET".to_string()),
      bulk(b""),
      Value::Integer(0),
      bulk(b"value"),
    ];
    let args = LSetCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "");
    assert_eq!(args.index, 0);
  }

  #[test]
  fn test_parse_args_empty_value() {
    let items = vec![
      Value::SimpleString("LSET".to_string()),
      bulk(b"mylist"),
      Value::Integer(0),
      bulk(b""),
    ];
    let args = LSetCommand::parse_args(&items).unwrap();
    assert_eq!(args.element, b"");
  }

  #[test]
  fn test_parse_args_nil_value() {
    let items = vec![
      Value::SimpleString("LSET".to_string()),
      bulk(b"mylist"),
      Value::Integer(0),
      Value::BulkString(None),
    ];
    let args = LSetCommand::parse_args(&items).unwrap();
    assert_eq!(args.element, b"");
  }

  #[test]
  fn test_parse_args_simplestring_value() {
    let items = vec![
      Value::SimpleString("LSET".to_string()),
      bulk(b"mylist"),
      Value::Integer(0),
      Value::SimpleString("hello".to_string()),
    ];
    let args = LSetCommand::parse_args(&items).unwrap();
    assert_eq!(args.element, b"hello");
  }

  #[test]
  fn test_parse_args_zero_index() {
    let items = vec![
      Value::SimpleString("LSET".to_string()),
      bulk(b"mylist"),
      Value::Integer(0),
      bulk(b"first"),
    ];
    let args = LSetCommand::parse_args(&items).unwrap();
    assert_eq!(args.index, 0);
  }

  #[test]
  fn test_parse_args_large_negative() {
    let items = vec![
      Value::SimpleString("LSET".to_string()),
      bulk(b"mylist"),
      Value::Integer(-1000000),
      bulk(b"value"),
    ];
    let args = LSetCommand::parse_args(&items).unwrap();
    assert_eq!(args.index, -1000000);
  }

  #[test]
  fn test_parse_args_binary_value() {
    let binary_data: Vec<u8> = (0..=255).collect();
    let items = vec![
      Value::SimpleString("LSET".to_string()),
      bulk(b"mylist"),
      Value::Integer(0),
      Value::BulkString(Some(binary_data.clone())),
    ];
    let args = LSetCommand::parse_args(&items).unwrap();
    assert_eq!(args.element, binary_data);
  }
}
