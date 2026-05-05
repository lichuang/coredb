//! LINDEX command implementation
//!
//! LINDEX key index
//! Returns the element at index in the list stored at key.
//!
//! The index is zero-based, so 0 means the first element, 1 the second, etc.
//! Negative indices can be used to designate elements starting at the tail:
//! -1 is the last element, -2 the second to last, etc.
//!
//! Return value:
//! - BulkString: the element at index
//! - Nil: if index is out of range or key does not exist
//! - Error: if key exists but is not a list

use crate::encoding::{ListElementValue, ListMetadata, TYPE_LIST};
use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

pub struct LIndexCommand;

struct LIndexArgs {
  key: String,
  index: i64,
}

impl LIndexCommand {
  fn parse_args(items: &[Value]) -> Result<LIndexArgs, ProtocolError> {
    if items.len() != 3 {
      return Err(ProtocolError::WrongArgCount("lindex"));
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::InvalidArgument("key")),
    };

    let index = parse_i64(&items[2])?;

    Ok(LIndexArgs { key, index })
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
impl Command for LIndexCommand {
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
            return Ok(Value::BulkString(None));
          }
          meta
        }
        Err(_) => return Ok(Value::BulkString(None)),
      },
      None => return Ok(Value::BulkString(None)),
    };

    // Resolve the logical index to a physical index
    let physical = match metadata.resolve_index(args.index) {
      Some(p) => p,
      None => return Ok(Value::BulkString(None)),
    };

    // Build sub-key and fetch the element
    let sub_key =
      ListElementValue::build_sub_key_hex(args.key.as_bytes(), metadata.version, physical);

    match server.get(&sub_key).await? {
      Some(raw_elem) => match ListElementValue::deserialize(&raw_elem) {
        Ok(elem) => Ok(Value::BulkString(Some(elem.data))),
        Err(_) => Ok(Value::BulkString(None)),
      },
      None => Ok(Value::BulkString(None)),
    }
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
      Value::SimpleString("LINDEX".to_string()),
      bulk(b"mylist"),
      Value::Integer(2),
    ];
    let args = LIndexCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mylist");
    assert_eq!(args.index, 2);
  }

  #[test]
  fn test_parse_args_valid_negative() {
    let items = vec![
      Value::SimpleString("LINDEX".to_string()),
      bulk(b"mylist"),
      Value::Integer(-1),
    ];
    let args = LIndexCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mylist");
    assert_eq!(args.index, -1);
  }

  #[test]
  fn test_parse_args_string_index() {
    let items = vec![
      Value::SimpleString("LINDEX".to_string()),
      bulk(b"mylist"),
      bulk(b"3"),
    ];
    let args = LIndexCommand::parse_args(&items).unwrap();
    assert_eq!(args.index, 3);
  }

  #[test]
  fn test_parse_args_too_few() {
    let items = vec![Value::SimpleString("LINDEX".to_string()), bulk(b"mylist")];
    assert!(LIndexCommand::parse_args(&items).is_err());
  }

  #[test]
  fn test_parse_args_too_many() {
    let items = vec![
      Value::SimpleString("LINDEX".to_string()),
      bulk(b"mylist"),
      Value::Integer(0),
      bulk(b"extra"),
    ];
    assert!(LIndexCommand::parse_args(&items).is_err());
  }

  #[test]
  fn test_parse_args_no_args() {
    let items = vec![Value::SimpleString("LINDEX".to_string())];
    assert!(LIndexCommand::parse_args(&items).is_err());
  }

  #[test]
  fn test_parse_args_invalid_index() {
    let items = vec![
      Value::SimpleString("LINDEX".to_string()),
      bulk(b"mylist"),
      bulk(b"abc"),
    ];
    assert!(LIndexCommand::parse_args(&items).is_err());
  }

  #[test]
  fn test_parse_args_empty_key() {
    let items = vec![
      Value::SimpleString("LINDEX".to_string()),
      bulk(b""),
      Value::Integer(0),
    ];
    let args = LIndexCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "");
    assert_eq!(args.index, 0);
  }

  #[test]
  fn test_parse_args_zero_index() {
    let items = vec![
      Value::SimpleString("LINDEX".to_string()),
      bulk(b"mylist"),
      Value::Integer(0),
    ];
    let args = LIndexCommand::parse_args(&items).unwrap();
    assert_eq!(args.index, 0);
  }

  #[test]
  fn test_parse_args_large_negative() {
    let items = vec![
      Value::SimpleString("LINDEX".to_string()),
      bulk(b"mylist"),
      Value::Integer(-1000000),
    ];
    let args = LIndexCommand::parse_args(&items).unwrap();
    assert_eq!(args.index, -1000000);
  }
}
