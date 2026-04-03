use crate::encoding::StringValue;
use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

/// Parameters for PSETEX command
#[derive(Debug, Clone, PartialEq)]
pub struct PsetexParams {
  pub key: String,
  pub milliseconds: u64,
  pub value: Vec<u8>,
}

impl PsetexParams {
  pub fn new(key: impl Into<String>, milliseconds: u64, value: impl Into<Vec<u8>>) -> Self {
    Self {
      key: key.into(),
      milliseconds,
      value: value.into(),
    }
  }

  fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
    if items.len() != 4 {
      return Err(ProtocolError::WrongArgCount("PSETEX"));
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::InvalidArgument("key")),
    };

    let milliseconds =
      parse_u64(&items[2]).ok_or(ProtocolError::InvalidArgument("milliseconds"))?;

    let value = match &items[3] {
      Value::BulkString(Some(data)) => data.clone(),
      Value::SimpleString(s) => s.as_bytes().to_vec(),
      _ => return Err(ProtocolError::InvalidArgument("value")),
    };

    Ok(PsetexParams::new(key, milliseconds, value))
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

/// PSETEX command executor
pub struct PsetexCommand;

#[async_trait]
impl Command for PsetexCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    let params = PsetexParams::parse(items)?;

    let now = now_ms();
    let expires_at = now + params.milliseconds;

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
  fn test_psetex_params_parse_basic() {
    let items = vec![
      Value::BulkString(Some(b"PSETEX".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"1000".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
    ];
    let params = PsetexParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
    assert_eq!(params.milliseconds, 1000);
    assert_eq!(params.value, b"myvalue");
  }

  #[test]
  fn test_psetex_params_parse_with_integer() {
    let items = vec![
      Value::BulkString(Some(b"PSETEX".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::Integer(5000),
      Value::BulkString(Some(b"myvalue".to_vec())),
    ];
    let params = PsetexParams::parse(&items).unwrap();
    assert_eq!(params.milliseconds, 5000);
  }

  #[test]
  fn test_psetex_params_parse_too_few_args() {
    let items = vec![
      Value::BulkString(Some(b"PSETEX".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"1000".to_vec())),
    ];
    assert!(PsetexParams::parse(&items).is_err());
  }

  #[test]
  fn test_psetex_params_parse_too_many_args() {
    let items = vec![
      Value::BulkString(Some(b"PSETEX".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"1000".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
      Value::BulkString(Some(b"extra".to_vec())),
    ];
    assert!(PsetexParams::parse(&items).is_err());
  }

  #[test]
  fn test_psetex_params_parse_invalid_milliseconds() {
    let items = vec![
      Value::BulkString(Some(b"PSETEX".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"notanumber".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
    ];
    assert!(PsetexParams::parse(&items).is_err());
  }

  #[test]
  fn test_psetex_params_parse_negative_milliseconds() {
    let items = vec![
      Value::BulkString(Some(b"PSETEX".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::Integer(-10),
      Value::BulkString(Some(b"myvalue".to_vec())),
    ];
    assert!(PsetexParams::parse(&items).is_err());
  }

  #[test]
  fn test_psetex_params_parse_zero_milliseconds() {
    let items = vec![
      Value::BulkString(Some(b"PSETEX".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"0".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
    ];
    let params = PsetexParams::parse(&items).unwrap();
    assert_eq!(params.milliseconds, 0);
  }

  #[test]
  fn test_psetex_params_parse_large_milliseconds() {
    let items = vec![
      Value::BulkString(Some(b"PSETEX".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"86400000".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
    ];
    let params = PsetexParams::parse(&items).unwrap();
    assert_eq!(params.milliseconds, 86400000);
  }
}
