//! SISMEMBER command implementation
//!
//! SISMEMBER key member
//! Returns if member is a member of the set stored at key.
//!
//! Return value:
//! - 1 if the element is a member of the set
//! - 0 if the element is not a member of the set, or if key does not exist
//! - Error if key exists but is not a set

use crate::encoding::{SetMemberValue, SetMetadata, TYPE_SET};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

/// SISMEMBER command handler
pub struct SIsMemberCommand;

#[async_trait]
impl Command for SIsMemberCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Value {
    // SISMEMBER key member (exactly 3 items: command + key + member)
    if items.len() != 3 {
      return Value::error("ERR wrong number of arguments for 'sismember' command");
    }

    // Parse key
    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Value::error("ERR invalid key"),
    };

    // Parse member
    let member = match &items[2] {
      Value::BulkString(Some(data)) => data.clone(),
      Value::SimpleString(s) => s.as_bytes().to_vec(),
      _ => return Value::error("ERR invalid member"),
    };

    // Get metadata
    let metadata = match server.get(&key).await {
      Ok(Some(raw_meta)) => match SetMetadata::deserialize(&raw_meta) {
        Ok(meta) => {
          if meta.get_type() != TYPE_SET {
            return Value::error(
              "WRONGTYPE Operation against a key holding the wrong kind of value",
            );
          }
          // Check if expired
          if meta.is_expired(now_ms()) {
            return Value::Integer(0);
          }
          meta
        }
        Err(_) => {
          return Value::Integer(0);
        }
      },
      _ => {
        // Key not found
        return Value::Integer(0);
      }
    };

    // Check if member exists by looking up the sub-key
    let sub_key_str = SetMemberValue::build_sub_key_hex(key.as_bytes(), metadata.version, &member);

    match server.get(&sub_key_str).await {
      Ok(Some(_)) => Value::Integer(1),
      _ => Value::Integer(0),
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
  fn test_parse_args_valid() {
    let items = vec![
      Value::SimpleString("SISMEMBER".to_string()),
      bulk(b"myset"),
      bulk(b"member1"),
    ];
    // 3 items = valid
    assert_eq!(items.len(), 3);
  }

  #[test]
  fn test_parse_args_insufficient_no_args() {
    let items = vec![Value::SimpleString("SISMEMBER".to_string())];
    assert_ne!(items.len(), 3);
  }

  #[test]
  fn test_parse_args_insufficient_no_member() {
    let items = vec![Value::SimpleString("SISMEMBER".to_string()), bulk(b"myset")];
    assert_ne!(items.len(), 3);
  }

  #[test]
  fn test_parse_args_too_many() {
    let items = vec![
      Value::SimpleString("SISMEMBER".to_string()),
      bulk(b"myset"),
      bulk(b"member1"),
      bulk(b"extra"),
    ];
    assert_ne!(items.len(), 3);
  }

  #[test]
  fn test_parse_args_invalid_key_type() {
    let items = vec![
      Value::SimpleString("SISMEMBER".to_string()),
      Value::Integer(42),
      bulk(b"member"),
    ];
    // Key is Integer, not BulkString or SimpleString
    match &items[1] {
      Value::BulkString(Some(_)) | Value::SimpleString(_) => panic!("expected non-string"),
      _ => {}
    }
  }

  #[test]
  fn test_parse_args_invalid_member_type() {
    let items = vec![
      Value::SimpleString("SISMEMBER".to_string()),
      bulk(b"myset"),
      Value::Integer(42),
    ];
    // Member is Integer, not BulkString or SimpleString
    match &items[2] {
      Value::BulkString(Some(_)) | Value::SimpleString(_) => panic!("expected non-string"),
      _ => {}
    }
  }

  #[test]
  fn test_parse_args_binary_member() {
    let items = vec![
      Value::SimpleString("SISMEMBER".to_string()),
      bulk(b"myset"),
      bulk(b"\x00\x01\xff"),
    ];
    assert_eq!(items.len(), 3);
  }

  #[test]
  fn test_parse_args_empty_member() {
    let items = vec![
      Value::SimpleString("SISMEMBER".to_string()),
      bulk(b"myset"),
      bulk(b""),
    ];
    assert_eq!(items.len(), 3);
  }

  #[test]
  fn test_parse_args_simple_string_key_and_member() {
    let items = vec![
      Value::SimpleString("SISMEMBER".to_string()),
      Value::SimpleString("myset".to_string()),
      Value::SimpleString("member".to_string()),
    ];
    assert_eq!(items.len(), 3);
  }
}
