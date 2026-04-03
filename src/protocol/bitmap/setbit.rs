use rockraft::raft::types::UpsertKV;

use crate::encoding::{BitmapFragment, BitmapMetadata, TYPE_BITMAP};
use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

struct SetBitArgs {
  key: String,
  offset: u64,
  bit_value: u8,
}

pub struct SetBitCommand;

impl SetBitCommand {
  fn parse_args(items: &[Value]) -> Result<SetBitArgs, ProtocolError> {
    if items.len() != 4 {
      return Err(ProtocolError::WrongArgCount("setbit"));
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::InvalidArgument("key")),
    };

    let offset = match &items[2] {
      Value::BulkString(Some(data)) => {
        let s = String::from_utf8_lossy(data);
        match s.parse::<u64>() {
          Ok(v) => v,
          Err(_) => {
            return Err(ProtocolError::Custom(
              "ERR bit offset is not an integer or out of range",
            ));
          }
        }
      }
      Value::SimpleString(s) => match s.parse::<u64>() {
        Ok(v) => v,
        Err(_) => {
          return Err(ProtocolError::Custom(
            "ERR bit offset is not an integer or out of range",
          ));
        }
      },
      Value::Integer(i) => {
        if *i < 0 {
          return Err(ProtocolError::Custom(
            "ERR bit offset is not an integer or out of range",
          ));
        }
        *i as u64
      }
      _ => {
        return Err(ProtocolError::Custom(
          "ERR bit offset is not an integer or out of range",
        ));
      }
    };

    let bit_value = match &items[3] {
      Value::BulkString(Some(data)) => {
        let s = String::from_utf8_lossy(data);
        match s.trim() {
          "0" => 0u8,
          "1" => 1u8,
          _ => {
            return Err(ProtocolError::Custom(
              "ERR bit is not an integer or out of range",
            ));
          }
        }
      }
      Value::SimpleString(s) => match s.trim() {
        "0" => 0u8,
        "1" => 1u8,
        _ => {
          return Err(ProtocolError::Custom(
            "ERR bit is not an integer or out of range",
          ));
        }
      },
      Value::Integer(i) => match *i {
        0 => 0u8,
        1 => 1u8,
        _ => {
          return Err(ProtocolError::Custom(
            "ERR bit is not an integer or out of range",
          ));
        }
      },
      _ => {
        return Err(ProtocolError::Custom(
          "ERR bit is not an integer or out of range",
        ));
      }
    };

    Ok(SetBitArgs {
      key,
      offset,
      bit_value,
    })
  }
}

#[async_trait]
impl Command for SetBitCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    let args = Self::parse_args(items)?;

    let mut metadata = match server.get(&args.key).await? {
      Some(raw_meta) => match BitmapMetadata::deserialize(&raw_meta) {
        Ok(meta) => {
          if meta.get_type() != TYPE_BITMAP {
            return Err(ProtocolError::WrongType.into());
          }
          if meta.is_expired(now_ms()) {
            BitmapMetadata::new()
          } else {
            meta
          }
        }
        Err(_) => BitmapMetadata::new(),
      },
      None => BitmapMetadata::new(),
    };

    let version = metadata.version;
    let frag_idx = BitmapFragment::fragment_index(args.offset);
    let offset_in_frag = BitmapFragment::offset_in_fragment(args.offset);

    let sub_key_str = BitmapFragment::build_sub_key_hex(args.key.as_bytes(), version, frag_idx);

    let mut fragment = match server.get(&sub_key_str).await? {
      Some(raw_frag) => raw_frag,
      None => BitmapFragment::empty_fragment(),
    };

    let old_bit = BitmapFragment::get_bit(&fragment, offset_in_frag);
    BitmapFragment::set_bit(&mut fragment, offset_in_frag, args.bit_value);

    if args.offset + 1 > metadata.size {
      metadata.size = args.offset + 1;
    }

    let entries: Vec<UpsertKV> = vec![
      UpsertKV::insert(sub_key_str, &fragment),
      UpsertKV::insert(args.key.clone(), &metadata.serialize()),
    ];

    server.batch_write(entries).await?;

    Ok(Value::Integer(old_bit as i64))
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
    let items = vec![ss("SETBIT"), bulk(b"mykey"), ss("100"), ss("1")];
    let args = SetBitCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "mykey");
    assert_eq!(args.offset, 100);
    assert_eq!(args.bit_value, 1);
  }

  #[test]
  fn test_parse_args_set_zero() {
    let items = vec![ss("SETBIT"), bulk(b"mykey"), ss("0"), ss("0")];
    let args = SetBitCommand::parse_args(&items).unwrap();
    assert_eq!(args.offset, 0);
    assert_eq!(args.bit_value, 0);
  }

  #[test]
  fn test_parse_args_large_offset() {
    let items = vec![ss("SETBIT"), bulk(b"mykey"), ss("4294967295"), ss("1")];
    let args = SetBitCommand::parse_args(&items).unwrap();
    assert_eq!(args.offset, 4294967295);
    assert_eq!(args.bit_value, 1);
  }

  #[test]
  fn test_parse_args_offset_as_integer() {
    let items = vec![ss("SETBIT"), bulk(b"mykey"), Value::Integer(42), ss("1")];
    let args = SetBitCommand::parse_args(&items).unwrap();
    assert_eq!(args.offset, 42);
  }

  #[test]
  fn test_parse_args_bit_as_integer() {
    let items = vec![ss("SETBIT"), bulk(b"mykey"), ss("10"), Value::Integer(0)];
    let args = SetBitCommand::parse_args(&items).unwrap();
    assert_eq!(args.bit_value, 0);
  }

  #[test]
  fn test_parse_args_insufficient() {
    let items = vec![ss("SETBIT"), bulk(b"mykey"), ss("10")];
    assert!(SetBitCommand::parse_args(&items).is_err());
  }

  #[test]
  fn test_parse_args_no_args() {
    let items = vec![ss("SETBIT")];
    assert!(SetBitCommand::parse_args(&items).is_err());
  }

  #[test]
  fn test_parse_args_invalid_offset() {
    let items = vec![ss("SETBIT"), bulk(b"mykey"), ss("abc"), ss("1")];
    assert!(SetBitCommand::parse_args(&items).is_err());
  }

  #[test]
  fn test_parse_args_negative_offset() {
    let items = vec![ss("SETBIT"), bulk(b"mykey"), Value::Integer(-1), ss("1")];
    assert!(SetBitCommand::parse_args(&items).is_err());
  }

  #[test]
  fn test_parse_args_invalid_bit() {
    let items = vec![ss("SETBIT"), bulk(b"mykey"), ss("10"), ss("2")];
    assert!(SetBitCommand::parse_args(&items).is_err());

    let items = vec![ss("SETBIT"), bulk(b"mykey"), ss("10"), ss("-1")];
    assert!(SetBitCommand::parse_args(&items).is_err());

    let items = vec![ss("SETBIT"), bulk(b"mykey"), ss("10"), Value::Integer(5)];
    assert!(SetBitCommand::parse_args(&items).is_err());
  }

  #[test]
  fn test_parse_args_invalid_key() {
    let items = vec![ss("SETBIT"), Value::Integer(42), ss("10"), ss("1")];
    assert!(SetBitCommand::parse_args(&items).is_err());
  }
}
