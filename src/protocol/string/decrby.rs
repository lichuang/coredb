use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::protocol::string::atomic_incr::atomic_incr;
use crate::server::Server;
use async_trait::async_trait;

#[derive(Debug, Clone, PartialEq)]
pub struct DecrbyParams {
  pub key: String,
  pub decrement: i64,
}

impl DecrbyParams {
  fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
    if items.len() != 3 {
      return Err(ProtocolError::WrongArgCount("DECRBY"));
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::InvalidArgument("key")),
    };

    let decrement = match &items[2] {
      Value::BulkString(Some(data)) => {
        let s = String::from_utf8_lossy(data);
        s.parse::<i64>()
          .map_err(|_| ProtocolError::InvalidArgument("decrement"))?
      }
      Value::SimpleString(s) => s
        .parse::<i64>()
        .map_err(|_| ProtocolError::InvalidArgument("decrement"))?,
      Value::Integer(i) => *i,
      _ => return Err(ProtocolError::InvalidArgument("decrement")),
    };

    Ok(DecrbyParams { key, decrement })
  }
}

pub struct DecrbyCommand;

#[async_trait]
impl Command for DecrbyCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    let params = DecrbyParams::parse(items)?;
    let new_int = atomic_incr(server, &params.key, -params.decrement).await?;
    Ok(Value::Integer(new_int))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_decrby_params_parse_success() {
    let items = vec![
      Value::BulkString(Some(b"DECRBY".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"5".to_vec())),
    ];
    let params = DecrbyParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
    assert_eq!(params.decrement, 5);
  }

  #[test]
  fn test_decrby_params_parse_negative() {
    let items = vec![
      Value::BulkString(Some(b"DECRBY".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"-10".to_vec())),
    ];
    let params = DecrbyParams::parse(&items).unwrap();
    assert_eq!(params.decrement, -10);
  }

  #[test]
  fn test_decrby_params_parse_zero() {
    let items = vec![
      Value::BulkString(Some(b"DECRBY".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"0".to_vec())),
    ];
    let params = DecrbyParams::parse(&items).unwrap();
    assert_eq!(params.decrement, 0);
  }

  #[test]
  fn test_decrby_params_parse_large_positive() {
    let items = vec![
      Value::BulkString(Some(b"DECRBY".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"9223372036854775807".to_vec())),
    ];
    let params = DecrbyParams::parse(&items).unwrap();
    assert_eq!(params.decrement, i64::MAX);
  }

  #[test]
  fn test_decrby_params_parse_large_negative() {
    let items = vec![
      Value::BulkString(Some(b"DECRBY".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"-9223372036854775808".to_vec())),
    ];
    let params = DecrbyParams::parse(&items).unwrap();
    assert_eq!(params.decrement, i64::MIN);
  }

  #[test]
  fn test_decrby_params_parse_integer_value() {
    let items = vec![
      Value::BulkString(Some(b"DECRBY".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::Integer(42),
    ];
    let params = DecrbyParams::parse(&items).unwrap();
    assert_eq!(params.decrement, 42);
  }

  #[test]
  fn test_decrby_params_parse_simple_string() {
    let items = vec![
      Value::SimpleString("DECRBY".to_string()),
      Value::SimpleString("mykey".to_string()),
      Value::SimpleString("100".to_string()),
    ];
    let params = DecrbyParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
    assert_eq!(params.decrement, 100);
  }

  #[test]
  fn test_decrby_params_parse_missing_key() {
    let items = vec![Value::BulkString(Some(b"DECRBY".to_vec()))];
    assert!(DecrbyParams::parse(&items).is_err());
  }

  #[test]
  fn test_decrby_params_parse_missing_decrement() {
    let items = vec![
      Value::BulkString(Some(b"DECRBY".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
    ];
    assert!(DecrbyParams::parse(&items).is_err());
  }

  #[test]
  fn test_decrby_params_parse_invalid_decrement() {
    let items = vec![
      Value::BulkString(Some(b"DECRBY".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"not_a_number".to_vec())),
    ];
    assert!(DecrbyParams::parse(&items).is_err());
  }

  #[test]
  fn test_decrby_params_parse_too_large() {
    let items = vec![
      Value::BulkString(Some(b"DECRBY".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"9223372036854775808".to_vec())),
    ];
    assert!(DecrbyParams::parse(&items).is_err());
  }

  #[test]
  fn test_decrby_params_parse_extra_args() {
    let items = vec![
      Value::BulkString(Some(b"DECRBY".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"5".to_vec())),
      Value::BulkString(Some(b"extra".to_vec())),
    ];
    assert!(DecrbyParams::parse(&items).is_err());
  }
}
