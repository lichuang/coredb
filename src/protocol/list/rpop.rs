use rockraft::raft::types::UpsertKV;

use crate::encoding::{ListElementValue, ListMetadata, TYPE_LIST};
use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

pub struct RPopCommand;

struct PopArgs {
  key: String,
  count: Option<u64>,
}

impl RPopCommand {
  fn parse_args(items: &[Value]) -> Result<PopArgs, ProtocolError> {
    if items.len() < 2 {
      return Err(ProtocolError::WrongArgCount("rpop"));
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::InvalidArgument("key")),
    };

    let count = if items.len() >= 3 {
      match &items[2] {
        Value::BulkString(Some(data)) => {
          let s = String::from_utf8_lossy(data);
          match s.parse::<u64>() {
            Ok(n) => Some(n),
            Err(_) => return Err(ProtocolError::NotAnInteger),
          }
        }
        Value::SimpleString(s) => match s.parse::<u64>() {
          Ok(n) => Some(n),
          Err(_) => return Err(ProtocolError::NotAnInteger),
        },
        Value::Integer(n) => {
          if *n < 0 {
            return Err(ProtocolError::NotAnInteger);
          }
          Some(*n as u64)
        }
        _ => {
          return Err(ProtocolError::NotAnInteger);
        }
      }
    } else {
      None
    };

    Ok(PopArgs { key, count })
  }
}

#[async_trait]
impl Command for RPopCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    let args = Self::parse_args(items)?;

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

    if metadata.size == 0 {
      return Ok(Value::BulkString(None));
    }

    let pop_count = args.count.unwrap_or(1).min(metadata.size);
    let version = metadata.version;
    let mut tail = metadata.tail;
    let mut entries: Vec<UpsertKV> = Vec::with_capacity(pop_count as usize + 1);
    let mut results: Vec<Value> = Vec::with_capacity(pop_count as usize);

    for _ in 0..pop_count {
      tail -= 1;
      let sub_key_str = ListElementValue::build_sub_key_hex(args.key.as_bytes(), version, tail);
      match server.get(&sub_key_str).await? {
        Some(raw_elem) => match ListElementValue::deserialize(&raw_elem) {
          Ok(elem) => {
            results.push(Value::BulkString(Some(elem.data)));
            entries.push(UpsertKV::delete(sub_key_str));
          }
          Err(_) => break,
        },
        None => break,
      }
    }

    let actual_popped = results.len() as u64;
    let new_size = metadata.size - actual_popped;

    if new_size == 0 {
      entries.push(UpsertKV::delete(args.key.clone()));
    } else {
      let mut new_meta = metadata.clone();
      new_meta.tail = tail;
      new_meta.size = new_size;
      entries.push(UpsertKV::insert(args.key.clone(), &new_meta.serialize()));
    }

    server.batch_write(entries).await?;

    Ok(match args.count {
      None => results
        .into_iter()
        .next()
        .unwrap_or(Value::BulkString(None)),
      Some(_) => Value::Array(Some(results)),
    })
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
    let items = vec![Value::SimpleString("RPOP".to_string()), bulk(b"mylist")];
    let args = RPopCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mylist");
    assert_eq!(args.count, None);
  }

  #[test]
  fn test_parse_args_with_count() {
    let items = vec![
      Value::SimpleString("RPOP".to_string()),
      bulk(b"mylist"),
      Value::SimpleString("3".to_string()),
    ];
    let args = RPopCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mylist");
    assert_eq!(args.count, Some(3));
  }

  #[test]
  fn test_parse_args_with_count_integer() {
    let items = vec![
      Value::SimpleString("RPOP".to_string()),
      bulk(b"mylist"),
      Value::Integer(5),
    ];
    let args = RPopCommand::parse_args(&items).unwrap();
    assert_eq!(args.count, Some(5));
  }

  #[test]
  fn test_parse_args_no_key() {
    let items = vec![Value::SimpleString("RPOP".to_string())];
    let result = RPopCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_invalid_count() {
    let items = vec![
      Value::SimpleString("RPOP".to_string()),
      bulk(b"mylist"),
      bulk(b"abc"),
    ];
    let result = RPopCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_negative_count() {
    let items = vec![
      Value::SimpleString("RPOP".to_string()),
      bulk(b"mylist"),
      Value::Integer(-1),
    ];
    let result = RPopCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_invalid_key_type() {
    let items = vec![Value::SimpleString("RPOP".to_string()), Value::Integer(42)];
    let result = RPopCommand::parse_args(&items);
    assert!(result.is_err());
  }
}
