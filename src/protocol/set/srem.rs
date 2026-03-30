use rockraft::raft::types::UpsertKV;

use crate::encoding::{SetMemberValue, SetMetadata, TYPE_SET};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

struct SRemArgs {
  key: String,
  members: Vec<Vec<u8>>,
}

pub struct SRemCommand;

impl SRemCommand {
  fn parse_args(items: &[Value]) -> Result<SRemArgs, Value> {
    if items.len() < 3 {
      return Err(Value::error(
        "ERR wrong number of arguments for 'srem' command",
      ));
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(Value::error("ERR invalid key")),
    };

    let mut members = Vec::with_capacity(items.len() - 2);
    for item in &items[2..] {
      let member = match item {
        Value::BulkString(Some(data)) => data.clone(),
        Value::SimpleString(s) => s.as_bytes().to_vec(),
        _ => return Err(Value::error("ERR invalid member")),
      };
      members.push(member);
    }

    Ok(SRemArgs { key, members })
  }
}

#[async_trait]
impl Command for SRemCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Value {
    let args = match Self::parse_args(items) {
      Ok(args) => args,
      Err(err) => return err,
    };

    let mut metadata = match server.get(&args.key).await {
      Ok(Some(raw_meta)) => match SetMetadata::deserialize(&raw_meta) {
        Ok(meta) => {
          if meta.get_type() != TYPE_SET {
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
    let mut removed_count = 0i64;
    let mut entries: Vec<UpsertKV> = Vec::new();

    for member in &args.members {
      let sub_key_str = SetMemberValue::build_sub_key_hex(args.key.as_bytes(), version, member);

      if let Ok(Some(_)) = server.get(&sub_key_str).await {
        entries.push(UpsertKV::delete(sub_key_str));
        removed_count += 1;
        metadata.decr_size();
      }
    }

    if removed_count == 0 {
      return Value::Integer(0);
    }

    entries.push(UpsertKV::insert(args.key.clone(), &metadata.serialize()));

    if let Err(e) = server.batch_write(entries).await {
      return Value::error(format!("ERR failed to batch write: {}", e));
    }

    Value::Integer(removed_count)
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
      Value::SimpleString("SREM".to_string()),
      bulk(b"myset"),
      bulk(b"member1"),
    ];

    let args = SRemCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "myset");
    assert_eq!(args.members.len(), 1);
    assert_eq!(args.members[0], b"member1");
  }

  #[test]
  fn test_parse_args_multiple_members() {
    let items = vec![
      Value::SimpleString("SREM".to_string()),
      bulk(b"myset"),
      bulk(b"a"),
      bulk(b"b"),
      bulk(b"c"),
    ];

    let args = SRemCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "myset");
    assert_eq!(args.members.len(), 3);
    assert_eq!(args.members[0], b"a");
    assert_eq!(args.members[1], b"b");
    assert_eq!(args.members[2], b"c");
  }

  #[test]
  fn test_parse_args_insufficient() {
    let items = vec![Value::SimpleString("SREM".to_string()), bulk(b"myset")];
    let result = SRemCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_no_args() {
    let items = vec![Value::SimpleString("SREM".to_string())];
    let result = SRemCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_simple_string_key() {
    let items = vec![
      Value::SimpleString("SREM".to_string()),
      Value::SimpleString("myset".to_string()),
      Value::SimpleString("member".to_string()),
    ];

    let args = SRemCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "myset");
    assert_eq!(args.members.len(), 1);
    assert_eq!(args.members[0], b"member");
  }

  #[test]
  fn test_parse_args_binary_member() {
    let items = vec![
      Value::SimpleString("SREM".to_string()),
      bulk(b"myset"),
      bulk(b"\x00\x01\xff"),
    ];

    let args = SRemCommand::parse_args(&items).unwrap();
    assert_eq!(args.members[0], b"\x00\x01\xff");
  }

  #[test]
  fn test_parse_args_empty_member() {
    let items = vec![
      Value::SimpleString("SREM".to_string()),
      bulk(b"myset"),
      bulk(b""),
    ];

    let args = SRemCommand::parse_args(&items).unwrap();
    assert_eq!(args.members.len(), 1);
    assert!(args.members[0].is_empty());
  }

  #[test]
  fn test_parse_args_invalid_key_type() {
    let items = vec![
      Value::SimpleString("SREM".to_string()),
      Value::Integer(42),
      bulk(b"member"),
    ];

    let result = SRemCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_invalid_member_type() {
    let items = vec![
      Value::SimpleString("SREM".to_string()),
      bulk(b"myset"),
      Value::Integer(42),
    ];

    let result = SRemCommand::parse_args(&items);
    assert!(result.is_err());
  }
}
