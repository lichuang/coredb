use crate::encoding::StringValue;
use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::protocol::string::set::set_if_not_exists;
use crate::server::Server;
use async_trait::async_trait;

/// Parameters for SETNX command
#[derive(Debug, Clone, PartialEq)]
pub struct SetnxParams {
  pub key: String,
  pub value: Vec<u8>,
}

impl SetnxParams {
  pub fn new(key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
    Self {
      key: key.into(),
      value: value.into(),
    }
  }

  fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
    if items.len() != 3 {
      return Err(ProtocolError::WrongArgCount("SETNX"));
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

    Ok(SetnxParams::new(key, value))
  }
}

/// SETNX command executor
pub struct SetnxCommand;

#[async_trait]
impl Command for SetnxCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    let params = SetnxParams::parse(items)?;

    let serialized = StringValue::new(params.value).serialize();
    let applied = set_if_not_exists(server, &params.key, serialized).await?;
    Ok(Value::Integer(i64::from(applied)))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_setnx_params_parse_basic() {
    let items = vec![
      Value::BulkString(Some(b"SETNX".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
    ];
    let params = SetnxParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
    assert_eq!(params.value, b"myvalue");
  }

  #[test]
  fn test_setnx_params_parse_simple_string() {
    let items = vec![
      Value::SimpleString("SETNX".to_string()),
      Value::SimpleString("mykey".to_string()),
      Value::SimpleString("myvalue".to_string()),
    ];
    let params = SetnxParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
    assert_eq!(params.value, b"myvalue");
  }

  #[test]
  fn test_setnx_params_parse_too_few_args() {
    let items = vec![
      Value::BulkString(Some(b"SETNX".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
    ];
    assert!(SetnxParams::parse(&items).is_err());
  }

  #[test]
  fn test_setnx_params_parse_too_many_args() {
    let items = vec![
      Value::BulkString(Some(b"SETNX".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
      Value::BulkString(Some(b"extra".to_vec())),
    ];
    assert!(SetnxParams::parse(&items).is_err());
  }

  #[test]
  fn test_setnx_params_parse_no_args() {
    let items = vec![Value::BulkString(Some(b"SETNX".to_vec()))];
    assert!(SetnxParams::parse(&items).is_err());
  }

  #[test]
  fn test_setnx_params_parse_empty_key() {
    let items = vec![
      Value::BulkString(Some(b"SETNX".to_vec())),
      Value::BulkString(Some(b"".to_vec())),
      Value::BulkString(Some(b"value".to_vec())),
    ];
    let params = SetnxParams::parse(&items).unwrap();
    assert_eq!(params.key, "");
    assert_eq!(params.value, b"value");
  }

  #[test]
  fn test_setnx_params_parse_empty_value() {
    let items = vec![
      Value::BulkString(Some(b"SETNX".to_vec())),
      Value::BulkString(Some(b"key".to_vec())),
      Value::BulkString(Some(b"".to_vec())),
    ];
    let params = SetnxParams::parse(&items).unwrap();
    assert_eq!(params.key, "key");
    assert_eq!(params.value, b"");
  }

  #[test]
  fn test_setnx_params_parse_binary_value() {
    let binary_data: Vec<u8> = vec![0x00, 0xFF, 0x80, 0x7F, 0x01, 0xFE];
    let items = vec![
      Value::BulkString(Some(b"SETNX".to_vec())),
      Value::BulkString(Some(b"binary_key".to_vec())),
      Value::BulkString(Some(binary_data.clone())),
    ];
    let params = SetnxParams::parse(&items).unwrap();
    assert_eq!(params.key, "binary_key");
    assert_eq!(params.value, binary_data);
  }

  #[test]
  fn test_setnx_params_parse_unicode_key() {
    let items = vec![
      Value::BulkString(Some(b"SETNX".to_vec())),
      Value::BulkString(Some("键".as_bytes().to_vec())),
      Value::BulkString(Some(b"value".to_vec())),
    ];
    let params = SetnxParams::parse(&items).unwrap();
    assert_eq!(params.key, "键");
    assert_eq!(params.value, b"value");
  }

  #[test]
  fn test_setnx_params_parse_unicode_value() {
    let items = vec![
      Value::BulkString(Some(b"SETNX".to_vec())),
      Value::BulkString(Some(b"key".to_vec())),
      Value::BulkString(Some("值🎉".as_bytes().to_vec())),
    ];
    let params = SetnxParams::parse(&items).unwrap();
    assert_eq!(params.key, "key");
    assert_eq!(params.value, "值🎉".as_bytes());
  }

  #[test]
  fn test_setnx_params_parse_large_value() {
    let large_value: Vec<u8> = (0..1024).map(|i| (i % 256) as u8).collect();
    let items = vec![
      Value::BulkString(Some(b"SETNX".to_vec())),
      Value::BulkString(Some(b"large_key".to_vec())),
      Value::BulkString(Some(large_value.clone())),
    ];
    let params = SetnxParams::parse(&items).unwrap();
    assert_eq!(params.key, "large_key");
    assert_eq!(params.value, large_value);
  }
}
