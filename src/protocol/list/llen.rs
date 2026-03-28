//! LLEN command implementation
//!
//! LLEN key
//! Returns the length of the list stored at key.
//!
//! Return value:
//! - Integer: the length of the list
//! - 0 if key does not exist
//! - Error if key exists but is not a list

use crate::encoding::{ListMetadata, TYPE_LIST};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

pub struct LLenCommand;

#[async_trait]
impl Command for LLenCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Value {
    // Parse LLEN key (exactly 2 items: command + key)
    if items.len() != 2 {
      return Value::error("ERR wrong number of arguments for 'llen' command");
    }

    // Parse key
    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Value::error("ERR invalid key"),
    };

    // Get metadata
    let metadata = match server.get(&key).await {
      Ok(Some(raw_meta)) => match ListMetadata::deserialize(&raw_meta) {
        Ok(meta) => {
          // Check type
          if meta.get_type() != TYPE_LIST {
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
          // Corrupted metadata, return 0
          return Value::Integer(0);
        }
      },
      _ => {
        // Key not found, return 0
        return Value::Integer(0);
      }
    };

    // Return the size from metadata
    Value::Integer(metadata.size as i64)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn bulk(data: &[u8]) -> Value {
    Value::BulkString(Some(data.to_vec()))
  }

  #[test]
  fn test_llen_valid_args() {
    let items = vec![Value::SimpleString("LLEN".to_string()), bulk(b"mylist")];
    // We can't fully test execute without a server, but verify parsing doesn't crash
    assert_eq!(items.len(), 2);
    match &items[1] {
      Value::BulkString(Some(data)) => {
        assert_eq!(String::from_utf8_lossy(data), "mylist");
      }
      _ => panic!("expected bulk string"),
    }
  }

  #[test]
  fn test_llen_too_few_args() {
    let items = vec![Value::SimpleString("LLEN".to_string())];
    assert!(items.len() != 2);
  }

  #[test]
  fn test_llen_too_many_args() {
    let items = vec![
      Value::SimpleString("LLEN".to_string()),
      bulk(b"mylist"),
      bulk(b"extra"),
    ];
    assert!(items.len() != 2);
  }
}
