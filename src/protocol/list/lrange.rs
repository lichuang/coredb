use crate::encoding::{ListElementValue, ListMetadata, TYPE_LIST};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

pub struct LRangeCommand;

struct RangeArgs {
  key: String,
  start: i64,
  stop: i64,
}

impl LRangeCommand {
  fn parse_args(items: &[Value]) -> Result<RangeArgs, Value> {
    if items.len() != 4 {
      return Err(Value::error(
        "ERR wrong number of arguments for 'lrange' command",
      ));
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(Value::error("ERR invalid key")),
    };

    let start = parse_i64(&items[2], "start")?;
    let stop = parse_i64(&items[3], "stop")?;

    Ok(RangeArgs { key, start, stop })
  }
}

fn parse_i64(value: &Value, name: &str) -> Result<i64, Value> {
  match value {
    Value::BulkString(Some(data)) => {
      let s = String::from_utf8_lossy(data);
      s.parse::<i64>()
        .map_err(|_| Value::error("ERR value is not an integer or out of range"))
    }
    Value::SimpleString(s) => s
      .parse::<i64>()
      .map_err(|_| Value::error("ERR value is not an integer or out of range")),
    Value::Integer(n) => Ok(*n),
    _ => Err(Value::error(format!("ERR invalid {}", name))),
  }
}

#[async_trait]
impl Command for LRangeCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Value {
    let args = match Self::parse_args(items) {
      Ok(args) => args,
      Err(err) => return err,
    };

    let metadata = match server.get(&args.key).await {
      Ok(Some(raw_meta)) => match ListMetadata::deserialize(&raw_meta) {
        Ok(meta) => {
          if meta.get_type() != TYPE_LIST {
            return Value::error(
              "WRONGTYPE Operation against a key holding the wrong kind of value",
            );
          }
          if meta.is_expired(now_ms()) {
            return Value::Array(Some(vec![]));
          }
          meta
        }
        Err(_) => return Value::Array(Some(vec![])),
      },
      _ => return Value::Array(Some(vec![])),
    };

    if metadata.size == 0 {
      return Value::Array(Some(vec![]));
    }

    let size = metadata.size as i64;

    let start = normalize_index(args.start, size);
    let stop = normalize_index(args.stop, size);

    if start > stop || start >= size {
      return Value::Array(Some(vec![]));
    }

    let end = stop.min(size - 1);
    let count = (end - start + 1) as usize;

    let mut results = Vec::with_capacity(count);
    let version = metadata.version;

    for i in start..=end {
      let physical = metadata.index_at(i as u64).unwrap();
      let sub_key = ListElementValue::build_sub_key_hex(args.key.as_bytes(), version, physical);

      match server.get(&sub_key).await {
        Ok(Some(raw_elem)) => match ListElementValue::deserialize(&raw_elem) {
          Ok(elem) => results.push(Value::BulkString(Some(elem.data))),
          Err(_) => break,
        },
        _ => break,
      }
    }

    Value::Array(Some(results))
  }
}

fn normalize_index(index: i64, size: i64) -> i64 {
  if index < 0 {
    (size + index).max(0)
  } else {
    index.min(size)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn bulk(data: &[u8]) -> Value {
    Value::BulkString(Some(data.to_vec()))
  }

  #[test]
  fn test_parse_args_valid() {
    let items = vec![
      Value::SimpleString("LRANGE".to_string()),
      bulk(b"mylist"),
      Value::Integer(0),
      Value::Integer(-1),
    ];
    let args = LRangeCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mylist");
    assert_eq!(args.start, 0);
    assert_eq!(args.stop, -1);
  }

  #[test]
  fn test_parse_args_string_indices() {
    let items = vec![
      Value::SimpleString("LRANGE".to_string()),
      bulk(b"mylist"),
      bulk(b"1"),
      bulk(b"10"),
    ];
    let args = LRangeCommand::parse_args(&items).unwrap();
    assert_eq!(args.start, 1);
    assert_eq!(args.stop, 10);
  }

  #[test]
  fn test_parse_args_too_few() {
    let items = vec![
      Value::SimpleString("LRANGE".to_string()),
      bulk(b"mylist"),
      Value::Integer(0),
    ];
    assert!(LRangeCommand::parse_args(&items).is_err());
  }

  #[test]
  fn test_parse_args_too_many() {
    let items = vec![
      Value::SimpleString("LRANGE".to_string()),
      bulk(b"mylist"),
      Value::Integer(0),
      Value::Integer(-1),
      bulk(b"extra"),
    ];
    assert!(LRangeCommand::parse_args(&items).is_err());
  }

  #[test]
  fn test_parse_args_no_key() {
    let items = vec![Value::SimpleString("LRANGE".to_string())];
    assert!(LRangeCommand::parse_args(&items).is_err());
  }

  #[test]
  fn test_parse_args_invalid_start() {
    let items = vec![
      Value::SimpleString("LRANGE".to_string()),
      bulk(b"mylist"),
      bulk(b"abc"),
      Value::Integer(-1),
    ];
    assert!(LRangeCommand::parse_args(&items).is_err());
  }

  #[test]
  fn test_parse_args_invalid_stop() {
    let items = vec![
      Value::SimpleString("LRANGE".to_string()),
      bulk(b"mylist"),
      Value::Integer(0),
      bulk(b"xyz"),
    ];
    assert!(LRangeCommand::parse_args(&items).is_err());
  }

  #[test]
  fn test_normalize_index_positive() {
    assert_eq!(normalize_index(0, 5), 0);
    assert_eq!(normalize_index(2, 5), 2);
    assert_eq!(normalize_index(4, 5), 4);
    assert_eq!(normalize_index(5, 5), 5);
    assert_eq!(normalize_index(100, 5), 5);
  }

  #[test]
  fn test_normalize_index_negative() {
    assert_eq!(normalize_index(-1, 5), 4);
    assert_eq!(normalize_index(-2, 5), 3);
    assert_eq!(normalize_index(-5, 5), 0);
    assert_eq!(normalize_index(-6, 5), 0);
    assert_eq!(normalize_index(-100, 5), 0);
  }
}
