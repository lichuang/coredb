use crate::encoding::{BitmapFragment, BitmapMetadata, TYPE_BITMAP};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

struct GetBitArgs {
  key: String,
  offset: u64,
}

pub struct GetBitCommand;

impl GetBitCommand {
  fn parse_args(items: &[Value]) -> Result<GetBitArgs, Value> {
    if items.len() != 3 {
      return Err(Value::error(
        "ERR wrong number of arguments for 'getbit' command",
      ));
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(Value::error("ERR invalid key")),
    };

    let offset = match &items[2] {
      Value::BulkString(Some(data)) => {
        let s = String::from_utf8_lossy(data);
        match s.parse::<u64>() {
          Ok(v) => v,
          Err(_) => {
            return Err(Value::error(
              "ERR bit offset is not an integer or out of range",
            ));
          }
        }
      }
      Value::SimpleString(s) => match s.parse::<u64>() {
        Ok(v) => v,
        Err(_) => {
          return Err(Value::error(
            "ERR bit offset is not an integer or out of range",
          ));
        }
      },
      Value::Integer(i) => {
        if *i < 0 {
          return Err(Value::error(
            "ERR bit offset is not an integer or out of range",
          ));
        }
        *i as u64
      }
      _ => {
        return Err(Value::error(
          "ERR bit offset is not an integer or out of range",
        ));
      }
    };

    Ok(GetBitArgs { key, offset })
  }
}

#[async_trait]
impl Command for GetBitCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Value {
    let args = match Self::parse_args(items) {
      Ok(args) => args,
      Err(err) => return err,
    };

    let metadata = match server.get(&args.key).await {
      Ok(Some(raw_meta)) => match BitmapMetadata::deserialize(&raw_meta) {
        Ok(meta) => {
          if meta.get_type() != TYPE_BITMAP {
            return Value::error(
              "WRONGTYPE Operation against a key holding the wrong kind of value",
            );
          }
          if meta.is_expired(now_ms()) {
            return Value::Integer(0);
          }
          meta
        }
        Err(_) => return Value::Integer(0),
      },
      _ => return Value::Integer(0),
    };

    let version = metadata.version;
    let frag_idx = BitmapFragment::fragment_index(args.offset);
    let offset_in_frag = BitmapFragment::offset_in_fragment(args.offset);

    let sub_key_str = BitmapFragment::build_sub_key_hex(args.key.as_bytes(), version, frag_idx);

    let fragment = match server.get(&sub_key_str).await {
      Ok(Some(raw_frag)) => raw_frag,
      _ => return Value::Integer(0),
    };

    Value::Integer(BitmapFragment::get_bit(&fragment, offset_in_frag) as i64)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn bulk(data: &[u8]) -> Value {
    Value::BulkString(Some(data.to_vec()))
  }

  fn ss(s: &str) -> Value {
    Value::SimpleString(s.to_string())
  }

  #[test]
  fn test_parse_args_basic() {
    let items = vec![ss("GETBIT"), bulk(b"mykey"), ss("100")];
    let args = GetBitCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mykey");
    assert_eq!(args.offset, 100);
  }

  #[test]
  fn test_parse_args_zero_offset() {
    let items = vec![ss("GETBIT"), bulk(b"mykey"), ss("0")];
    let args = GetBitCommand::parse_args(&items).unwrap();
    assert_eq!(args.offset, 0);
  }

  #[test]
  fn test_parse_args_large_offset() {
    let items = vec![ss("GETBIT"), bulk(b"mykey"), ss("4294967295")];
    let args = GetBitCommand::parse_args(&items).unwrap();
    assert_eq!(args.offset, 4294967295);
  }

  #[test]
  fn test_parse_args_offset_as_integer() {
    let items = vec![ss("GETBIT"), bulk(b"mykey"), Value::Integer(42)];
    let args = GetBitCommand::parse_args(&items).unwrap();
    assert_eq!(args.offset, 42);
  }

  #[test]
  fn test_parse_args_insufficient() {
    let items = vec![ss("GETBIT"), bulk(b"mykey")];
    assert!(GetBitCommand::parse_args(&items).is_err());
  }

  #[test]
  fn test_parse_args_no_args() {
    let items = vec![ss("GETBIT")];
    assert!(GetBitCommand::parse_args(&items).is_err());
  }

  #[test]
  fn test_parse_args_too_many() {
    let items = vec![ss("GETBIT"), bulk(b"mykey"), ss("10"), ss("extra")];
    assert!(GetBitCommand::parse_args(&items).is_err());
  }

  #[test]
  fn test_parse_args_invalid_offset() {
    let items = vec![ss("GETBIT"), bulk(b"mykey"), ss("abc")];
    assert!(GetBitCommand::parse_args(&items).is_err());
  }

  #[test]
  fn test_parse_args_negative_offset() {
    let items = vec![ss("GETBIT"), bulk(b"mykey"), Value::Integer(-1)];
    assert!(GetBitCommand::parse_args(&items).is_err());
  }

  #[test]
  fn test_parse_args_invalid_key() {
    let items = vec![ss("GETBIT"), Value::Integer(42), ss("10")];
    assert!(GetBitCommand::parse_args(&items).is_err());
  }
}
