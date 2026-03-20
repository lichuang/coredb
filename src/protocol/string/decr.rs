use crate::encoding::StringValue;
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
  fn parse(items: &[Value]) -> Option<Self> {
    if items.len() != 2 {
      return None;
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return None,
    };

    Some(DecrParams { key })
  }
}

/// Parse bytes as i64, returns None if invalid
fn parse_i64(data: &[u8]) -> Option<i64> {
  let s = std::str::from_utf8(data).ok()?;
  s.trim().parse::<i64>().ok()
}

/// DECR command executor
pub struct DecrCommand;

#[async_trait]
impl Command for DecrCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Value {
    let params = match DecrParams::parse(items) {
      Some(params) => params,
      None => return Value::error("ERR wrong number of arguments for 'decr' command"),
    };

    let now = now_ms();

    // Get current value
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
        Err(_) => {
          return Value::error("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
      },
      Ok(None) => None,
      Err(_) => None,
    };

    // Parse current integer value or use 0 for non-existent
    let current_int: i64 = match current_value {
      Some(ref sv) => match parse_i64(&sv.data) {
        Some(n) => n,
        None => return Value::error("ERR value is not an integer or out of range"),
      },
      None => 0,
    };

    // Decrement and check overflow
    let new_int = match current_int.checked_sub(1) {
      Some(n) => n,
      None => return Value::error("ERR increment or decrement would overflow"),
    };

    // Preserve existing TTL if key existed and had one
    let new_string_value = if let Some(ref sv) = current_value {
      if sv.has_expiration() {
        StringValue::with_expiration(new_int.to_string().into_bytes(), sv.expires_at)
      } else {
        StringValue::new(new_int.to_string().into_bytes())
      }
    } else {
      StringValue::new(new_int.to_string().into_bytes())
    };

    // Store the new value
    let serialized = new_string_value.serialize();
    match server.set(params.key, serialized).await {
      Ok(_) => Value::Integer(new_int),
      Err(e) => Value::error(format!("ERR {}", e)),
    }
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
    assert!(DecrParams::parse(&items).is_none());
  }

  #[test]
  fn test_decr_params_parse_too_many_args() {
    let items = vec![
      Value::BulkString(Some(b"DECR".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"extra".to_vec())),
    ];
    assert!(DecrParams::parse(&items).is_none());
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
    assert_eq!(parse_i64(b"9223372036854775808"), None); // Overflow
    assert_eq!(parse_i64(b"-9223372036854775809"), None); // Underflow
  }
}
