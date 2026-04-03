use crate::encoding::StringValue;
use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

/// Parameters for SETEX command
#[derive(Debug, Clone, PartialEq)]
pub struct SetexParams {
  pub key: String,
  pub seconds: u64,
  pub value: Vec<u8>,
}

impl SetexParams {
  pub fn new(key: impl Into<String>, seconds: u64, value: impl Into<Vec<u8>>) -> Self {
    Self {
      key: key.into(),
      seconds,
      value: value.into(),
    }
  }

  fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
    if items.len() != 4 {
      return Err(ProtocolError::WrongArgCount("SETEX"));
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::InvalidArgument("key")),
    };

    let seconds = parse_u64(&items[2]).ok_or(ProtocolError::InvalidArgument("seconds"))?;

    let value = match &items[3] {
      Value::BulkString(Some(data)) => data.clone(),
      Value::SimpleString(s) => s.as_bytes().to_vec(),
      _ => return Err(ProtocolError::InvalidArgument("value")),
    };

    Ok(SetexParams::new(key, seconds, value))
  }
}

fn parse_u64(value: &Value) -> Option<u64> {
  match value {
    Value::BulkString(Some(data)) => String::from_utf8_lossy(data).parse::<u64>().ok(),
    Value::SimpleString(s) => s.parse::<u64>().ok(),
    Value::Integer(i) if *i >= 0 => Some(*i as u64),
    _ => None,
  }
}

/// SETEX command executor
pub struct SetexCommand;

#[async_trait]
impl Command for SetexCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    let params = SetexParams::parse(items)?;

    let now = now_ms();
    let expires_at = now + params.seconds * 1000;

    let string_value = StringValue::with_expiration(params.value, expires_at);
    let serialized = string_value.serialize();

    server.set(params.key, serialized).await?;
    Ok(Value::ok())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_setex_params_parse_basic() {
    let items = vec![
      Value::BulkString(Some(b"SETEX".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"10".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
    ];
    let params = SetexParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
    assert_eq!(params.seconds, 10);
    assert_eq!(params.value, b"myvalue");
  }

  #[test]
  fn test_setex_params_parse_with_integer() {
    let items = vec![
      Value::BulkString(Some(b"SETEX".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::Integer(60),
      Value::BulkString(Some(b"myvalue".to_vec())),
    ];
    let params = SetexParams::parse(&items).unwrap();
    assert_eq!(params.seconds, 60);
  }

  #[test]
  fn test_setex_params_parse_too_few_args() {
    let items = vec![
      Value::BulkString(Some(b"SETEX".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"10".to_vec())),
    ];
    assert!(SetexParams::parse(&items).is_err());
  }

  #[test]
  fn test_setex_params_parse_too_many_args() {
    let items = vec![
      Value::BulkString(Some(b"SETEX".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"10".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
      Value::BulkString(Some(b"extra".to_vec())),
    ];
    assert!(SetexParams::parse(&items).is_err());
  }

  #[test]
  fn test_setex_params_parse_invalid_seconds() {
    let items = vec![
      Value::BulkString(Some(b"SETEX".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"notanumber".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
    ];
    assert!(SetexParams::parse(&items).is_err());
  }

  #[test]
  fn test_setex_params_parse_negative_seconds() {
    let items = vec![
      Value::BulkString(Some(b"SETEX".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::Integer(-10),
      Value::BulkString(Some(b"myvalue".to_vec())),
    ];
    assert!(SetexParams::parse(&items).is_err());
  }

  #[test]
  fn test_setex_params_parse_zero_seconds() {
    let items = vec![
      Value::BulkString(Some(b"SETEX".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"0".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
    ];
    let params = SetexParams::parse(&items).unwrap();
    assert_eq!(params.seconds, 0);
  }

  #[test]
  fn test_setex_params_parse_large_seconds() {
    let items = vec![
      Value::BulkString(Some(b"SETEX".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"86400".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
    ];
    let params = SetexParams::parse(&items).unwrap();
    assert_eq!(params.seconds, 86400);
  }
}
