//! HELLO command implementation
//!
//! HELLO [proto [AUTH username password] [SETNAME client-name]]
//!
//! Used by redis-py 8.0+ during connection to negotiate RESP protocol version.
//! Supports both RESP2 (proto=2) and RESP3 (proto=3).

use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use async_trait::async_trait;

pub struct HelloCommand;

#[async_trait]
impl Command for HelloCommand {
  async fn execute(&self, items: &[Value], _server: &Server) -> Result<Value, CoreDbError> {
    if items.len() > 6 {
      return Err(ProtocolError::WrongArgCount("hello").into());
    }

    let requested_proto = if items.len() >= 2 {
      let proto = match &items[1] {
        Value::BulkString(Some(data)) => String::from_utf8_lossy(data).parse::<u8>().ok(),
        Value::SimpleString(s) => s.parse::<u8>().ok(),
        Value::Integer(i) => Some(*i as u8),
        _ => None,
      };

      match proto {
        Some(2) | Some(3) => proto.unwrap(),
        Some(_) => {
          return Err(ProtocolError::InvalidArgument("protocol version is not supported").into());
        }
        None => {
          return Err(ProtocolError::InvalidArgument("protocol version").into());
        }
      }
    } else {
      2
    };

    let response = Value::Map(vec![
      (
        Value::BulkString(Some(b"server".to_vec())),
        Value::BulkString(Some(b"coredb".to_vec())),
      ),
      (
        Value::BulkString(Some(b"version".to_vec())),
        Value::BulkString(Some(b"1.0.0".to_vec())),
      ),
      (
        Value::BulkString(Some(b"proto".to_vec())),
        Value::Integer(requested_proto as i64),
      ),
      (Value::BulkString(Some(b"id".to_vec())), Value::Integer(1)),
      (
        Value::BulkString(Some(b"mode".to_vec())),
        Value::BulkString(Some(b"standalone".to_vec())),
      ),
      (
        Value::BulkString(Some(b"role".to_vec())),
        Value::BulkString(Some(b"master".to_vec())),
      ),
      (
        Value::BulkString(Some(b"modules".to_vec())),
        Value::BulkString(Some(b"".to_vec())),
      ),
    ]);

    Ok(response)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_hello_map_structure() {
    let response = Value::Map(vec![
      (
        Value::BulkString(Some(b"server".to_vec())),
        Value::BulkString(Some(b"coredb".to_vec())),
      ),
      (
        Value::BulkString(Some(b"proto".to_vec())),
        Value::Integer(2),
      ),
    ]);

    match &response {
      Value::Map(pairs) => {
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].0, Value::BulkString(Some(b"server".to_vec())));
        assert_eq!(pairs[1].1, Value::Integer(2));
      }
      _ => panic!("Expected map"),
    }
  }

  #[test]
  fn test_hello_encode_resp2_flat_array() {
    let response = Value::Map(vec![
      (
        Value::BulkString(Some(b"server".to_vec())),
        Value::BulkString(Some(b"coredb".to_vec())),
      ),
      (
        Value::BulkString(Some(b"proto".to_vec())),
        Value::Integer(2),
      ),
    ]);

    let encoded = response.encode_proto(2);
    assert!(encoded.starts_with(b"*4\r\n"));
  }

  #[test]
  fn test_hello_encode_resp3_map() {
    let response = Value::Map(vec![
      (
        Value::BulkString(Some(b"server".to_vec())),
        Value::BulkString(Some(b"coredb".to_vec())),
      ),
      (
        Value::BulkString(Some(b"proto".to_vec())),
        Value::Integer(3),
      ),
    ]);

    let encoded = response.encode_proto(3);
    assert!(encoded.starts_with(b"%2\r\n"));
  }
}
