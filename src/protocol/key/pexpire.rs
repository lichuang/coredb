//! PEXPIRE command implementation
//!
//! PEXPIRE key milliseconds [NX | XX | GT | LT]
//!
//! Set a timeout on key in milliseconds. After the timeout has expired,
//! the key will automatically be deleted. Works on any data type.

use crate::encoding::{HashMetadata, StringValue};
use crate::error::{CoreDbError, EncodeError, ProtocolError};
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
  fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
    // Need at least: PEXPIRE key milliseconds (3 items)
    if items.len() < 3 {
      return Err(ProtocolError::WrongArgCount("pexpire"));
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::WrongArgCount("pexpire")),
    };

    let milliseconds = parse_i64(&items[2]).ok_or(ProtocolError::NotAnInteger)?;

    // Parse optional condition flag
    let condition = if items.len() >= 4 {
      let flag = match &items[3] {
        Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_uppercase(),
        Value::SimpleString(s) => s.to_uppercase(),
        _ => return Err(ProtocolError::WrongArgCount("pexpire")),
      };

      match flag.as_str() {
        "NX" => Some(ExpireCondition::Nx),
        "XX" => Some(ExpireCondition::Xx),
        "GT" => Some(ExpireCondition::Gt),
        "LT" => Some(ExpireCondition::Lt),
        _ => return Err(ProtocolError::SyntaxError),
      }
    } else {
      None
    };

    Ok(PexpireParams {
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
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    let params = PexpireParams::parse(items)?;

    // Negative milliseconds: delete the key immediately
    if params.milliseconds < 0 {
      server.delete(&params.key).await?;
      return Ok(Value::Integer(1));
    }

    // Read current key state
    let key_state = read_key_state(server, &params.key).await?;

    // Key must exist (not expired or missing)
    let (current_expires_at, flags) = match key_state {
      KeyState::NotFound | KeyState::Expired => return Ok(Value::Integer(0)),
      KeyState::String(sv) => (sv.expires_at, sv.flags),
      KeyState::Hash(hm) => (hm.expires_at, hm.flags),
    };

    // Calculate new expiration timestamp in milliseconds
    let now = now_ms();
    let new_expires_at = now + params.milliseconds as u64;

    // Zero milliseconds: delete the key immediately
    if params.milliseconds == 0 {
      server.delete(&params.key).await?;
      return Ok(Value::Integer(1));
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
      return Ok(Value::Integer(0));
    }

    // Re-read and reconstruct the value with updated expiration
    let raw_value = match server.get(&params.key).await? {
      Some(v) => v,
      None => return Ok(Value::Integer(0)), // Key disappeared between reads
    };

    let data_type = flags & 0x0F;
    let new_value = if data_type == crate::encoding::TYPE_STRING {
      let mut sv = StringValue::deserialize(&raw_value)
        .map_err(|_| EncodeError::DeserializeFailed(format!("key '{}'", params.key)))?;
      sv.expires_at = new_expires_at;
      sv.serialize()
    } else if data_type == crate::encoding::TYPE_HASH {
      let mut hm = HashMetadata::deserialize(&raw_value)
        .map_err(|_| EncodeError::DeserializeFailed(format!("key '{}'", params.key)))?;
      hm.expires_at = new_expires_at;
      hm.serialize()
    } else {
      return Err(ProtocolError::Custom("ERR unsupported key type").into());
    };

    // Write the updated value back
    server.set(params.key, new_value).await?;
    Ok(Value::Integer(1))
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
    let items = vec![
      Value::BulkString(Some(b"PEXPIRE".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
    ];
    assert!(PexpireParams::parse(&items).is_err());

    let items = vec![Value::BulkString(Some(b"PEXPIRE".to_vec()))];
    assert!(PexpireParams::parse(&items).is_err());

    let items: Vec<Value> = vec![];
    assert!(PexpireParams::parse(&items).is_err());
  }

  #[test]
  fn test_pexpire_params_parse_invalid_milliseconds() {
    let items = vec![
      Value::BulkString(Some(b"PEXPIRE".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"not_a_number".to_vec())),
    ];
    assert!(PexpireParams::parse(&items).is_err());
  }

  #[test]
  fn test_pexpire_params_parse_invalid_key_type() {
    let items = vec![
      Value::BulkString(Some(b"PEXPIRE".to_vec())),
      Value::Integer(123),
      Value::BulkString(Some(b"60000".to_vec())),
    ];
    assert!(PexpireParams::parse(&items).is_err());
  }

  #[test]
  fn test_pexpire_params_parse_unknown_option() {
    let items = vec![
      Value::BulkString(Some(b"PEXPIRE".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"60000".to_vec())),
      Value::BulkString(Some(b"UNKNOWN".to_vec())),
    ];
    assert!(PexpireParams::parse(&items).is_err());
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
