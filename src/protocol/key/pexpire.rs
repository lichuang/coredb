//! PEXPIRE command implementation
//!
//! PEXPIRE key milliseconds [NX | XX | GT | LT]
//!
//! Set a timeout on key in milliseconds. After the timeout has expired,
//! the key will automatically be deleted. Works on any data type
//! (string, hash, etc.).
//!
//! Options:
//! - NX: Only set the expiration if the key has no associated expiration
//! - XX: Only set the expiration if the key already has an expiration
//! - GT: Only set the expiration if the new expiration is greater than current one
//! - LT: Only set the expiration if the new expiration is less than current one
//!
//! Returns:
//! - Integer 1: the timeout was set
//! - Integer 0: the timeout was not set (key does not exist, or condition not met)

use crate::encoding::{HashMetadata, StringValue};
use crate::protocol::command::Command;
use crate::protocol::key::expire::{ExpireCondition, KeyState, read_key_state};
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

/// PEXPIRE command parameters
#[derive(Debug, Clone, PartialEq)]
pub struct PexpireParams {
  pub key: String,
  pub milliseconds: i64,
  pub condition: Option<ExpireCondition>,
}

/// Parse a Value as i64 (supports negative values for PEXPIRE)
fn parse_i64(value: &Value) -> Option<i64> {
  match value {
    Value::BulkString(Some(data)) => String::from_utf8_lossy(data).parse::<i64>().ok(),
    Value::SimpleString(s) => s.parse::<i64>().ok(),
    Value::Integer(i) => Some(*i),
    _ => None,
  }
}

impl PexpireParams {
  /// Parse PEXPIRE command parameters from RESP array items
  /// Format: PEXPIRE key milliseconds [NX | XX | GT | LT]
  fn parse(items: &[Value]) -> Option<Self> {
    // Need at least: PEXPIRE key milliseconds (3 items)
    if items.len() < 3 {
      return None;
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return None,
    };

    let milliseconds = parse_i64(&items[2])?;

    // Parse optional condition flag
    let condition = if items.len() >= 4 {
      let flag = match &items[3] {
        Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_uppercase(),
        Value::SimpleString(s) => s.to_uppercase(),
        _ => return None,
      };

      match flag.as_str() {
        "NX" => Some(ExpireCondition::Nx),
        "XX" => Some(ExpireCondition::Xx),
        "GT" => Some(ExpireCondition::Gt),
        "LT" => Some(ExpireCondition::Lt),
        _ => return None,
      }
    } else {
      None
    };

    Some(PexpireParams {
      key,
      milliseconds,
      condition,
    })
  }
}

/// PEXPIRE command executor
pub struct PexpireCommand;

#[async_trait]
impl Command for PexpireCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Value {
    let params = match PexpireParams::parse(items) {
      Some(params) => params,
      None => return Value::error("ERR wrong number of arguments for 'pexpire' command"),
    };

    // Negative milliseconds: delete the key immediately
    if params.milliseconds < 0 {
      match server.delete(&params.key).await {
        Ok(_) => return Value::Integer(1),
        Err(e) => return Value::error(format!("ERR failed to delete key: {}", e)),
      }
    }

    // Read current key state
    let key_state = match read_key_state(server, &params.key).await {
      Ok(state) => state,
      Err(e) => return Value::error(format!("ERR failed to read key '{}': {}", params.key, e)),
    };

    // Key must exist (not expired or missing)
    let (current_expires_at, flags) = match key_state {
      KeyState::NotFound | KeyState::Expired => return Value::Integer(0),
      KeyState::String(sv) => (sv.expires_at, sv.flags),
      KeyState::Hash(hm) => (hm.expires_at, hm.flags),
    };

    // Calculate new expiration timestamp in milliseconds
    let now = now_ms();
    let new_expires_at = now + params.milliseconds as u64;

    // Zero milliseconds: delete the key immediately
    if params.milliseconds == 0 {
      match server.delete(&params.key).await {
        Ok(_) => return Value::Integer(1),
        Err(e) => return Value::error(format!("ERR failed to delete key: {}", e)),
      }
    }

    // Check expire condition
    let should_set = match &params.condition {
      None => true,
      Some(ExpireCondition::Nx) => current_expires_at == 0,
      Some(ExpireCondition::Xx) => current_expires_at != 0,
      Some(ExpireCondition::Gt) => {
        if current_expires_at == 0 {
          false
        } else {
          new_expires_at > current_expires_at
        }
      }
      Some(ExpireCondition::Lt) => {
        if current_expires_at == 0 {
          false
        } else {
          new_expires_at < current_expires_at
        }
      }
    };

    if !should_set {
      return Value::Integer(0);
    }

    // Re-read and reconstruct the value with updated expiration
    let raw_value = match server.get(&params.key).await {
      Ok(Some(v)) => v,
      Ok(None) => return Value::Integer(0), // Key disappeared between reads
      Err(e) => return Value::error(format!("ERR failed to read key '{}': {}", params.key, e)),
    };

    let data_type = flags & 0x0F;
    let new_value = if data_type == crate::encoding::TYPE_STRING {
      match StringValue::deserialize(&raw_value) {
        Ok(mut sv) => {
          sv.expires_at = new_expires_at;
          sv.serialize()
        }
        Err(_) => return Value::error(format!("ERR failed to deserialize key '{}'", params.key)),
      }
    } else if data_type == crate::encoding::TYPE_HASH {
      match HashMetadata::deserialize(&raw_value) {
        Ok(mut hm) => {
          hm.expires_at = new_expires_at;
          hm.serialize()
        }
        Err(_) => return Value::error(format!("ERR failed to deserialize key '{}'", params.key)),
      }
    } else {
      return Value::error(format!("ERR unsupported key type for '{}'", params.key));
    };

    // Write the updated value back
    match server.set(params.key, new_value).await {
      Ok(_) => Value::Integer(1),
      Err(e) => Value::error(format!("ERR failed to set key: {}", e)),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_pexpire_params_parse_basic() {
    let items = vec![
      Value::BulkString(Some(b"PEXPIRE".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"60000".to_vec())),
    ];
    let params = PexpireParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
    assert_eq!(params.milliseconds, 60000);
    assert_eq!(params.condition, None);
  }

  #[test]
  fn test_pexpire_params_parse_with_nx() {
    let items = vec![
      Value::BulkString(Some(b"PEXPIRE".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"60000".to_vec())),
      Value::BulkString(Some(b"NX".to_vec())),
    ];
    let params = PexpireParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
    assert_eq!(params.milliseconds, 60000);
    assert_eq!(params.condition, Some(ExpireCondition::Nx));
  }

  #[test]
  fn test_pexpire_params_parse_with_xx() {
    let items = vec![
      Value::BulkString(Some(b"PEXPIRE".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"60000".to_vec())),
      Value::BulkString(Some(b"XX".to_vec())),
    ];
    let params = PexpireParams::parse(&items).unwrap();
    assert_eq!(params.condition, Some(ExpireCondition::Xx));
  }

  #[test]
  fn test_pexpire_params_parse_with_gt() {
    let items = vec![
      Value::BulkString(Some(b"PEXPIRE".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"60000".to_vec())),
      Value::BulkString(Some(b"GT".to_vec())),
    ];
    let params = PexpireParams::parse(&items).unwrap();
    assert_eq!(params.condition, Some(ExpireCondition::Gt));
  }

  #[test]
  fn test_pexpire_params_parse_with_lt() {
    let items = vec![
      Value::BulkString(Some(b"PEXPIRE".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"60000".to_vec())),
      Value::BulkString(Some(b"LT".to_vec())),
    ];
    let params = PexpireParams::parse(&items).unwrap();
    assert_eq!(params.condition, Some(ExpireCondition::Lt));
  }

  #[test]
  fn test_pexpire_params_parse_insufficient_args() {
    // Only PEXPIRE key (missing milliseconds)
    let items = vec![
      Value::BulkString(Some(b"PEXPIRE".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
    ];
    assert!(PexpireParams::parse(&items).is_none());

    // Only PEXPIRE
    let items = vec![Value::BulkString(Some(b"PEXPIRE".to_vec()))];
    assert!(PexpireParams::parse(&items).is_none());

    // Empty
    let items: Vec<Value> = vec![];
    assert!(PexpireParams::parse(&items).is_none());
  }

  #[test]
  fn test_pexpire_params_parse_invalid_milliseconds() {
    let items = vec![
      Value::BulkString(Some(b"PEXPIRE".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"not_a_number".to_vec())),
    ];
    assert!(PexpireParams::parse(&items).is_none());
  }

  #[test]
  fn test_pexpire_params_parse_invalid_key_type() {
    let items = vec![
      Value::BulkString(Some(b"PEXPIRE".to_vec())),
      Value::Integer(123),
      Value::BulkString(Some(b"60000".to_vec())),
    ];
    assert!(PexpireParams::parse(&items).is_none());
  }

  #[test]
  fn test_pexpire_params_parse_unknown_option() {
    let items = vec![
      Value::BulkString(Some(b"PEXPIRE".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"60000".to_vec())),
      Value::BulkString(Some(b"UNKNOWN".to_vec())),
    ];
    assert!(PexpireParams::parse(&items).is_none());
  }

  #[test]
  fn test_pexpire_params_parse_with_simple_string() {
    let items = vec![
      Value::SimpleString("PEXPIRE".to_string()),
      Value::SimpleString("mykey".to_string()),
      Value::SimpleString("120000".to_string()),
      Value::SimpleString("NX".to_string()),
    ];
    let params = PexpireParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
    assert_eq!(params.milliseconds, 120000);
    assert_eq!(params.condition, Some(ExpireCondition::Nx));
  }

  #[test]
  fn test_pexpire_params_parse_milliseconds_as_integer() {
    let items = vec![
      Value::BulkString(Some(b"PEXPIRE".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::Integer(300000),
    ];
    let params = PexpireParams::parse(&items).unwrap();
    assert_eq!(params.milliseconds, 300000);
  }

  #[test]
  fn test_pexpire_params_parse_negative_milliseconds() {
    let items = vec![
      Value::BulkString(Some(b"PEXPIRE".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::Integer(-1),
    ];
    let params = PexpireParams::parse(&items).unwrap();
    assert_eq!(params.milliseconds, -1);
  }

  #[test]
  fn test_pexpire_params_parse_zero_milliseconds() {
    let items = vec![
      Value::BulkString(Some(b"PEXPIRE".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::Integer(0),
    ];
    let params = PexpireParams::parse(&items).unwrap();
    assert_eq!(params.milliseconds, 0);
  }
}
