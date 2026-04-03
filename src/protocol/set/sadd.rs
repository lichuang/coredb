use rockraft::raft::types::UpsertKV;

use crate::encoding::{SetMemberValue, SetMetadata, TYPE_SET};
use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

struct SAddArgs {
  key: String,
  members: Vec<Vec<u8>>,
}

pub struct SAddCommand;

impl SAddCommand {
  fn parse_args(items: &[Value]) -> Result<SAddArgs, ProtocolError> {
    if items.len() < 3 {
      return Err(ProtocolError::WrongArgCount("sadd"));
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::InvalidArgument("key")),
    };

    let mut members = Vec::with_capacity(items.len() - 2);
    for item in &items[2..] {
      let member = match item {
        Value::BulkString(Some(data)) => data.clone(),
        Value::SimpleString(s) => s.as_bytes().to_vec(),
        _ => return Err(ProtocolError::InvalidArgument("member")),
      };
      members.push(member);
    }

    Ok(SAddArgs { key, members })
  }
}

#[async_trait]
impl Command for SAddCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    let args = Self::parse_args(items)?;

    let mut metadata = match server.get(&args.key).await? {
      Some(raw_meta) => match SetMetadata::deserialize(&raw_meta) {
        Ok(meta) => {
          if meta.get_type() != TYPE_SET {
            return Err(ProtocolError::WrongType.into());
          }
          if meta.is_expired(now_ms()) {
            SetMetadata::new()
          } else {
            meta
          }
        }
        Err(_) => SetMetadata::new(),
      },
      None => SetMetadata::new(),
    };

    let version = metadata.version;
    let mut added_count = 0i64;
    let mut entries: Vec<UpsertKV> = Vec::with_capacity(args.members.len() + 1);

    for member in &args.members {
      let sub_key_str = SetMemberValue::build_sub_key_hex(args.key.as_bytes(), version, member);

      let member_exists = (server.get(&sub_key_str).await?).is_some();

      let member_value = SetMemberValue;
      entries.push(UpsertKV::insert(sub_key_str, &member_value.serialize()));

      if !member_exists {
        metadata.incr_size();
        added_count += 1;
      }
    }

    entries.push(UpsertKV::insert(args.key.clone(), &metadata.serialize()));

    server.batch_write(entries).await?;

    Ok(Value::Integer(added_count))
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
      Value::SimpleString("SADD".to_string()),
      bulk(b"myset"),
      bulk(b"member1"),
    ];

    let args = SAddCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "myset");
    assert_eq!(args.members.len(), 1);
    assert_eq!(args.members[0], b"member1");
  }

  #[test]
  fn test_parse_args_multiple_members() {
    let items = vec![
      Value::SimpleString("SADD".to_string()),
      bulk(b"myset"),
      bulk(b"a"),
      bulk(b"b"),
      bulk(b"c"),
    ];

    let args = SAddCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "myset");
    assert_eq!(args.members.len(), 3);
    assert_eq!(args.members[0], b"a");
    assert_eq!(args.members[1], b"b");
    assert_eq!(args.members[2], b"c");
  }

  #[test]
  fn test_parse_args_insufficient() {
    let items = vec![Value::SimpleString("SADD".to_string()), bulk(b"myset")];
    let result = SAddCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_no_args() {
    let items = vec![Value::SimpleString("SADD".to_string())];
    let result = SAddCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_simple_string_key() {
    let items = vec![
      Value::SimpleString("SADD".to_string()),
      Value::SimpleString("myset".to_string()),
      Value::SimpleString("value".to_string()),
    ];

    let args = SAddCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "myset");
    assert_eq!(args.members.len(), 1);
    assert_eq!(args.members[0], b"value");
  }

  #[test]
  fn test_parse_args_binary_member() {
    let items = vec![
      Value::SimpleString("SADD".to_string()),
      bulk(b"myset"),
      bulk(b"\x00\x01\xff"),
    ];

    let args = SAddCommand::parse_args(&items).unwrap();
    assert_eq!(args.members[0], b"\x00\x01\xff");
  }

  #[test]
  fn test_parse_args_empty_member() {
    let items = vec![
      Value::SimpleString("SADD".to_string()),
      bulk(b"myset"),
      bulk(b""),
    ];

    let args = SAddCommand::parse_args(&items).unwrap();
    assert_eq!(args.members.len(), 1);
    assert!(args.members[0].is_empty());
  }

  #[test]
  fn test_parse_args_invalid_key_type() {
    let items = vec![
      Value::SimpleString("SADD".to_string()),
      Value::Integer(42),
      bulk(b"member"),
    ];

    let result = SAddCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_invalid_member_type() {
    let items = vec![
      Value::SimpleString("SADD".to_string()),
      bulk(b"myset"),
      Value::Integer(42),
    ];

    let result = SAddCommand::parse_args(&items);
    assert!(result.is_err());
  }
}
