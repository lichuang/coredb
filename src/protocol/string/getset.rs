use crate::encoding::StringValue;
use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

#[derive(Debug, Clone, PartialEq)]
pub struct GetSetParams {
  pub key: String,
  pub value: Vec<u8>,
}

impl GetSetParams {
  pub fn new(key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
    Self {
      key: key.into(),
      value: value.into(),
    }
  }

  fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
    if items.len() != 3 {
      return Err(ProtocolError::WrongArgCount("GETSET"));
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::InvalidArgument("key")),
    };

    let value = match &items[2] {
      Value::BulkString(Some(data)) => data.clone(),
      Value::SimpleString(s) => s.as_bytes().to_vec(),
      _ => return Err(ProtocolError::InvalidArgument("value")),
    };

    Ok(GetSetParams::new(key, value))
  }
}

pub struct GetSetCommand;

#[async_trait]
impl Command for GetSetCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    let params = GetSetParams::parse(items)?;

    let string_value = StringValue::new(params.value.clone());
    let serialized = string_value.serialize();

    let old_raw = server.getset(&params.key, serialized).await?;

    let now = now_ms();
    match old_raw {
      Some(raw) => {
        let string_value = match StringValue::deserialize(&raw) {
          Ok(v) => v,
          Err(_) => {
            // Not a StringValue — restore the original value and return WRONGTYPE
            server.set(params.key, raw).await?;
            return Err(ProtocolError::WrongType.into());
          }
        };
        if string_value.is_expired(now) {
          Ok(Value::BulkString(None))
        } else {
          Ok(Value::BulkString(Some(string_value.data)))
        }
      }
      None => Ok(Value::BulkString(None)),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_getset_params_parse_basic() {
    let items = vec![
      Value::BulkString(Some(b"GETSET".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
    ];
    let params = GetSetParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
    assert_eq!(params.value, b"myvalue");
  }

  #[test]
  fn test_getset_params_parse_simple_string() {
    let items = vec![
      Value::SimpleString("GETSET".to_string()),
      Value::SimpleString("mykey".to_string()),
      Value::SimpleString("myvalue".to_string()),
    ];
    let params = GetSetParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
    assert_eq!(params.value, b"myvalue");
  }

  #[test]
  fn test_getset_params_parse_too_few_args() {
    let items = vec![
      Value::BulkString(Some(b"GETSET".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
    ];
    assert!(GetSetParams::parse(&items).is_err());
  }

  #[test]
  fn test_getset_params_parse_too_many_args() {
    let items = vec![
      Value::BulkString(Some(b"GETSET".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
      Value::BulkString(Some(b"extra".to_vec())),
    ];
    assert!(GetSetParams::parse(&items).is_err());
  }

  #[test]
  fn test_getset_params_parse_no_args() {
    let items = vec![Value::BulkString(Some(b"GETSET".to_vec()))];
    assert!(GetSetParams::parse(&items).is_err());
  }

  #[test]
  fn test_getset_params_parse_empty_key() {
    let items = vec![
      Value::BulkString(Some(b"GETSET".to_vec())),
      Value::BulkString(Some(b"".to_vec())),
      Value::BulkString(Some(b"value".to_vec())),
    ];
    let params = GetSetParams::parse(&items).unwrap();
    assert_eq!(params.key, "");
    assert_eq!(params.value, b"value");
  }

  #[test]
  fn test_getset_params_parse_empty_value() {
    let items = vec![
      Value::BulkString(Some(b"GETSET".to_vec())),
      Value::BulkString(Some(b"key".to_vec())),
      Value::BulkString(Some(b"".to_vec())),
    ];
    let params = GetSetParams::parse(&items).unwrap();
    assert_eq!(params.key, "key");
    assert_eq!(params.value, b"");
  }

  #[test]
  fn test_getset_params_parse_binary_value() {
    let binary_data: Vec<u8> = vec![0x00, 0xFF, 0x80, 0x7F, 0x01, 0xFE];
    let items = vec![
      Value::BulkString(Some(b"GETSET".to_vec())),
      Value::BulkString(Some(b"binary_key".to_vec())),
      Value::BulkString(Some(binary_data.clone())),
    ];
    let params = GetSetParams::parse(&items).unwrap();
    assert_eq!(params.key, "binary_key");
    assert_eq!(params.value, binary_data);
  }

  #[test]
  fn test_getset_params_parse_unicode_key() {
    let items = vec![
      Value::BulkString(Some(b"GETSET".to_vec())),
      Value::BulkString(Some("键".as_bytes().to_vec())),
      Value::BulkString(Some(b"value".to_vec())),
    ];
    let params = GetSetParams::parse(&items).unwrap();
    assert_eq!(params.key, "键");
    assert_eq!(params.value, b"value");
  }

  #[test]
  fn test_getset_params_parse_unicode_value() {
    let items = vec![
      Value::BulkString(Some(b"GETSET".to_vec())),
      Value::BulkString(Some(b"key".to_vec())),
      Value::BulkString(Some("值".as_bytes().to_vec())),
    ];
    let params = GetSetParams::parse(&items).unwrap();
    assert_eq!(params.key, "key");
    assert_eq!(params.value, "值".as_bytes());
  }

  #[test]
  fn test_getset_params_parse_large_value() {
    let large_value: Vec<u8> = (0..1024).map(|i| (i % 256) as u8).collect();
    let items = vec![
      Value::BulkString(Some(b"GETSET".to_vec())),
      Value::BulkString(Some(b"large_key".to_vec())),
      Value::BulkString(Some(large_value.clone())),
    ];
    let params = GetSetParams::parse(&items).unwrap();
    assert_eq!(params.key, "large_key");
    assert_eq!(params.value, large_value);
  }
}
