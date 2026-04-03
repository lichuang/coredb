use crate::encoding::StringValue;
use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

/// Parameters for DECR command
#[derive(Debug, Clone, PartialEq)]
pub struct DecrParams {
  pub key: String,
}

impl DecrParams {
  fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
    if items.len() != 2 {
      return Err(ProtocolError::WrongArgCount("DECR"));
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::InvalidArgument("key")),
    };

    Ok(DecrParams { key })
  }
}

fn parse_i64(data: &[u8]) -> Option<i64> {
  let s = std::str::from_utf8(data).ok()?;
  s.trim().parse::<i64>().ok()
}

/// DECR command executor
pub struct DecrCommand;

#[async_trait]
impl Command for DecrCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    let params = DecrParams::parse(items)?;

    let now = now_ms();

    let current_value = match server.get(&params.key).await {
      Ok(Some(raw_value)) => match StringValue::deserialize(&raw_value) {
        Ok(string_value) => {
          if string_value.is_expired(now) {
            let _ = server.delete(&params.key).await;
            None
          } else {
            Some(string_value)
          }
        }
        Err(_) => return Err(ProtocolError::WrongType.into()),
      },
      Ok(None) => None,
      Err(_) => None,
    };

    let current_int: i64 = match current_value {
      Some(ref sv) => match parse_i64(&sv.data) {
        Some(n) => n,
        None => return Err(ProtocolError::NotAnInteger.into()),
      },
      None => 0,
    };

    let new_int = match current_int.checked_sub(1) {
      Some(n) => n,
      None => return Err(ProtocolError::Overflow.into()),
    };

    let new_string_value = if let Some(ref sv) = current_value {
      if sv.has_expiration() {
        StringValue::with_expiration(new_int.to_string().into_bytes(), sv.expires_at)
      } else {
        StringValue::new(new_int.to_string().into_bytes())
      }
    } else {
      StringValue::new(new_int.to_string().into_bytes())
    };

    let serialized = new_string_value.serialize();
    server.set(params.key, serialized).await?;
    Ok(Value::Integer(new_int))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_decr_params_parse_success() {
    let items = vec![
      Value::BulkString(Some(b"DECR".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
    ];
    let params = DecrParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
  }

  #[test]
  fn test_decr_params_parse_simple_string() {
    let items = vec![
      Value::SimpleString("DECR".to_string()),
      Value::SimpleString("mykey".to_string()),
    ];
    let params = DecrParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
  }

  #[test]
  fn test_decr_params_parse_no_key() {
    let items = vec![Value::BulkString(Some(b"DECR".to_vec()))];
    assert!(DecrParams::parse(&items).is_err());
  }

  #[test]
  fn test_decr_params_parse_too_many_args() {
    let items = vec![
      Value::BulkString(Some(b"DECR".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"extra".to_vec())),
    ];
    assert!(DecrParams::parse(&items).is_err());
  }

  #[test]
  fn test_parse_i64_valid() {
    assert_eq!(parse_i64(b"0"), Some(0));
    assert_eq!(parse_i64(b"1"), Some(1));
    assert_eq!(parse_i64(b"-1"), Some(-1));
    assert_eq!(parse_i64(b"12345"), Some(12345));
    assert_eq!(parse_i64(b"-12345"), Some(-12345));
    assert_eq!(parse_i64(b"9223372036854775807"), Some(i64::MAX));
    assert_eq!(parse_i64(b"-9223372036854775808"), Some(i64::MIN));
  }

  #[test]
  fn test_parse_i64_with_whitespace() {
    assert_eq!(parse_i64(b" 123 "), Some(123));
    assert_eq!(parse_i64(b"\t456\n"), Some(456));
  }

  #[test]
  fn test_parse_i64_invalid() {
    assert_eq!(parse_i64(b""), None);
    assert_eq!(parse_i64(b"  "), None);
    assert_eq!(parse_i64(b"abc"), None);
    assert_eq!(parse_i64(b"12abc"), None);
    assert_eq!(parse_i64(b"9223372036854775808"), None);
    assert_eq!(parse_i64(b"-9223372036854775809"), None);
  }
}
