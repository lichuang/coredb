use crate::encoding::{NO_EXPIRATION, StringValue};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

/// Expiration time options for SET command
#[derive(Debug, Clone, PartialEq)]
pub enum Expiration {
  /// EX seconds - Set the specified expire time, in seconds
  Ex(u64),
  /// PX milliseconds - Set the specified expire time, in milliseconds
  Px(u64),
  /// EXAT timestamp-seconds - Set the specified Unix time at which the key will expire, in seconds
  ExAt(u64),
  /// PXAT timestamp-milliseconds - Set the specified Unix time at which the key will expire, in milliseconds
  PxAt(u64),
  /// KEEPTTL - Retain the time to live associated with the key
  KeepTTL,
}

/// Set mode options (NX/XX)
#[derive(Debug, Clone, PartialEq)]
pub enum SetMode {
  /// NX - Only set the key if it does not already exist
  Nx,
  /// XX - Only set the key if it already exists
  Xx,
}

/// Parameters for SET command
///
/// Standard Redis SET command format:
/// SET key value [NX | XX] [GET] [EX seconds | PX milliseconds | EXAT timestamp | PXAT milliseconds-timestamp | KEEPTTL]
#[derive(Debug, Clone, PartialEq)]
pub struct SetParams {
  /// The key to set
  pub key: String,
  /// The value to set
  pub value: Vec<u8>,
  /// NX or XX mode (optional)
  pub mode: Option<SetMode>,
  /// Whether to return the previous value (GET option)
  pub get: bool,
  /// Expiration time options (optional)
  pub expiration: Option<Expiration>,
}

impl SetParams {
  /// Create a new SetParams with minimal required fields
  pub fn new(key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
    Self {
      key: key.into(),
      value: value.into(),
      mode: None,
      get: false,
      expiration: None,
    }
  }

  /// Parse SET command parameters from RESP array items
  /// Format: SET key value [NX | XX] [GET] [EX seconds | PX milliseconds | EXAT timestamp | PXAT milliseconds-timestamp | KEEPTTL]
  fn parse(items: &[Value]) -> Option<Self> {
    // Minimum: SET key value
    if items.len() < 3 {
      return None;
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return None,
    };

    let value = match &items[2] {
      Value::BulkString(Some(data)) => data.clone(),
      Value::SimpleString(s) => s.as_bytes().to_vec(),
      _ => return None,
    };

    let mut params = SetParams::new(key, value);
    let mut i = 3;

    // Parse optional arguments
    while i < items.len() {
      let arg = match &items[i] {
        Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_uppercase(),
        Value::SimpleString(s) => s.to_uppercase(),
        _ => return None,
      };

      match arg.as_str() {
        "NX" => {
          if params.mode.is_some() {
            return None; // NX and XX are mutually exclusive
          }
          params.mode = Some(SetMode::Nx);
          i += 1;
        }
        "XX" => {
          if params.mode.is_some() {
            return None; // NX and XX are mutually exclusive
          }
          params.mode = Some(SetMode::Xx);
          i += 1;
        }
        "GET" => {
          params.get = true;
          i += 1;
        }
        "EX" => {
          if params.expiration.is_some() || i + 1 >= items.len() {
            return None;
          }
          let seconds = parse_u64(&items[i + 1])?;
          params.expiration = Some(Expiration::Ex(seconds));
          i += 2;
        }
        "PX" => {
          if params.expiration.is_some() || i + 1 >= items.len() {
            return None;
          }
          let milliseconds = parse_u64(&items[i + 1])?;
          params.expiration = Some(Expiration::Px(milliseconds));
          i += 2;
        }
        "EXAT" => {
          if params.expiration.is_some() || i + 1 >= items.len() {
            return None;
          }
          let timestamp = parse_u64(&items[i + 1])?;
          params.expiration = Some(Expiration::ExAt(timestamp));
          i += 2;
        }
        "PXAT" => {
          if params.expiration.is_some() || i + 1 >= items.len() {
            return None;
          }
          let timestamp = parse_u64(&items[i + 1])?;
          params.expiration = Some(Expiration::PxAt(timestamp));
          i += 2;
        }
        "KEEPTTL" => {
          if params.expiration.is_some() {
            return None;
          }
          params.expiration = Some(Expiration::KeepTTL);
          i += 1;
        }
        _ => return None, // Unknown option
      }
    }

    Some(params)
  }
}

/// Parse a Value as u64
fn parse_u64(value: &Value) -> Option<u64> {
  match value {
    Value::BulkString(Some(data)) => String::from_utf8_lossy(data).parse::<u64>().ok(),
    Value::SimpleString(s) => s.parse::<u64>().ok(),
    Value::Integer(i) if *i >= 0 => Some(*i as u64),
    _ => None,
  }
}

/// SET command executor
pub struct SetCommand;

#[async_trait]
impl Command for SetCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Value {
    let params = match SetParams::parse(items) {
      Some(params) => params,
      None => return Value::error("ERR wrong number of arguments for 'set' command"),
    };

    // Get current timestamp once for all expiration calculations
    let now = now_ms();

    // Calculate expiration timestamp in milliseconds (0 means no expiration)
    let expires_at = match params.expiration {
      Some(Expiration::KeepTTL) => {
        // Read existing value to get its expiration time
        match server.get(&params.key).await {
          Ok(Some(raw_value)) => {
            match StringValue::deserialize(&raw_value) {
              Ok(existing) if existing.is_expired(now) => NO_EXPIRATION, // Expired, no TTL to keep
              Ok(existing) if existing.has_expiration() => existing.expires_at, // Keep existing TTL
              Ok(_) => NO_EXPIRATION,  // No existing expiration to keep
              Err(_) => NO_EXPIRATION, // Corrupted, no TTL to keep
            }
          }
          _ => NO_EXPIRATION, // Key not found or error, no TTL to keep
        }
      }
      Some(exp) => match exp {
        Expiration::Ex(seconds) => now + seconds * 1000,
        Expiration::Px(millis) => now + millis,
        Expiration::ExAt(timestamp) => timestamp * 1000,
        Expiration::PxAt(timestamp) => timestamp,
        Expiration::KeepTTL => unreachable!(), // Handled above
      },
      None => NO_EXPIRATION, // No expiration
    };

    // Create StringValue and serialize
    let string_value = if expires_at == NO_EXPIRATION {
      StringValue::new(params.value)
    } else {
      StringValue::with_expiration(params.value, expires_at)
    };
    let serialized = string_value.serialize();

    // Check if key exists and get old value (for NX/XX/GET logic)
    // We need to track both: (1) if key exists for NX/XX logic, (2) the actual value for GET option
    enum ExistingKey {
      None,                     // Key doesn't exist
      Expired,                  // Key exists but is expired
      ValidString(StringValue), // Key exists and is a valid string
      OtherType,                // Key exists but is not a string (Hash, etc.)
    }

    let existing_key = match server.get(&params.key).await {
      Ok(Some(raw_value)) => {
        match StringValue::deserialize(&raw_value) {
          Ok(value) if !value.is_expired(now) => ExistingKey::ValidString(value),
          Ok(_) => ExistingKey::Expired, // Expired, treat as not exists
          Err(_) => ExistingKey::OtherType, // Not a StringValue (might be Hash or other type)
        }
      }
      _ => ExistingKey::None, // Not found or error
    };

    // Check if key exists (for NX/XX logic)
    let key_exists = matches!(
      existing_key,
      ExistingKey::ValidString(_) | ExistingKey::OtherType
    );

    // Apply NX/XX mode logic
    match params.mode {
      Some(SetMode::Nx) => {
        // NX: Only set if key does not exist
        if key_exists {
          // Key exists, do not set
          return if params.get {
            // GET with NX: return current value if it's a string, otherwise nil
            match existing_key {
              ExistingKey::ValidString(v) => Value::BulkString(Some(v.data)),
              _ => Value::BulkString(None), // Other type, return nil
            }
          } else {
            // Just return nil (nil bulk string)
            Value::BulkString(None)
          };
        }
      }
      Some(SetMode::Xx) => {
        // XX: Only set if key exists
        if !key_exists {
          // Key does not exist, do not set
          return if params.get {
            // GET with XX: return nil since key doesn't exist
            Value::BulkString(None)
          } else {
            Value::BulkString(None)
          };
        }
      }
      None => {
        // No mode restriction, always set
      }
    }

    // Store the old value for GET option before we overwrite
    let old_value_data = match existing_key {
      ExistingKey::ValidString(v) => Some(v.data),
      _ => None, // Return nil for expired, non-existent, or other types
    };

    // Set the new value
    match server.set(params.key, serialized).await {
      Ok(_) => {
        if params.get {
          // Return the previous value (or nil if key didn't exist)
          Value::BulkString(old_value_data)
        } else {
          Value::ok()
        }
      }
      Err(e) => Value::error(format!("ERR {}", e)),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_set_params_parse_basic() {
    let items = vec![
      Value::BulkString(Some(b"SET".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
    ];
    let params = SetParams::parse(&items).unwrap();
    assert_eq!(params.key, "mykey");
    assert_eq!(params.value, b"myvalue");
    assert_eq!(params.mode, None);
    assert_eq!(params.get, false);
    assert_eq!(params.expiration, None);
  }

  #[test]
  fn test_set_params_parse_with_nx() {
    let items = vec![
      Value::BulkString(Some(b"SET".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
      Value::BulkString(Some(b"NX".to_vec())),
    ];
    let params = SetParams::parse(&items).unwrap();
    assert_eq!(params.mode, Some(SetMode::Nx));
  }

  #[test]
  fn test_set_params_parse_with_xx() {
    let items = vec![
      Value::BulkString(Some(b"SET".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
      Value::BulkString(Some(b"XX".to_vec())),
    ];
    let params = SetParams::parse(&items).unwrap();
    assert_eq!(params.mode, Some(SetMode::Xx));
  }

  #[test]
  fn test_set_params_parse_with_get() {
    let items = vec![
      Value::BulkString(Some(b"SET".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
      Value::BulkString(Some(b"GET".to_vec())),
    ];
    let params = SetParams::parse(&items).unwrap();
    assert_eq!(params.get, true);
  }

  #[test]
  fn test_set_params_parse_with_ex() {
    let items = vec![
      Value::BulkString(Some(b"SET".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
      Value::BulkString(Some(b"EX".to_vec())),
      Value::BulkString(Some(b"60".to_vec())),
    ];
    let params = SetParams::parse(&items).unwrap();
    assert_eq!(params.expiration, Some(Expiration::Ex(60)));
  }

  #[test]
  fn test_set_params_parse_with_px() {
    let items = vec![
      Value::BulkString(Some(b"SET".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
      Value::BulkString(Some(b"PX".to_vec())),
      Value::BulkString(Some(b"1000".to_vec())),
    ];
    let params = SetParams::parse(&items).unwrap();
    assert_eq!(params.expiration, Some(Expiration::Px(1000)));
  }

  #[test]
  fn test_set_params_parse_with_exat() {
    let items = vec![
      Value::BulkString(Some(b"SET".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
      Value::BulkString(Some(b"EXAT".to_vec())),
      Value::BulkString(Some(b"1893456000".to_vec())),
    ];
    let params = SetParams::parse(&items).unwrap();
    assert_eq!(params.expiration, Some(Expiration::ExAt(1893456000)));
  }

  #[test]
  fn test_set_params_parse_with_pxat() {
    let items = vec![
      Value::BulkString(Some(b"SET".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
      Value::BulkString(Some(b"PXAT".to_vec())),
      Value::BulkString(Some(b"1893456000000".to_vec())),
    ];
    let params = SetParams::parse(&items).unwrap();
    assert_eq!(params.expiration, Some(Expiration::PxAt(1893456000000)));
  }

  #[test]
  fn test_set_params_parse_with_keepttl() {
    let items = vec![
      Value::BulkString(Some(b"SET".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
      Value::BulkString(Some(b"KEEPTTL".to_vec())),
    ];
    let params = SetParams::parse(&items).unwrap();
    assert_eq!(params.expiration, Some(Expiration::KeepTTL));
  }

  #[test]
  fn test_set_params_parse_combined_options() {
    let items = vec![
      Value::BulkString(Some(b"SET".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
      Value::BulkString(Some(b"NX".to_vec())),
      Value::BulkString(Some(b"GET".to_vec())),
      Value::BulkString(Some(b"EX".to_vec())),
      Value::BulkString(Some(b"60".to_vec())),
    ];
    let params = SetParams::parse(&items).unwrap();
    assert_eq!(params.mode, Some(SetMode::Nx));
    assert_eq!(params.get, true);
    assert_eq!(params.expiration, Some(Expiration::Ex(60)));
  }

  #[test]
  fn test_set_params_parse_nx_xx_mutually_exclusive() {
    let items = vec![
      Value::BulkString(Some(b"SET".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
      Value::BulkString(Some(b"NX".to_vec())),
      Value::BulkString(Some(b"XX".to_vec())),
    ];
    assert!(SetParams::parse(&items).is_none());
  }

  #[test]
  fn test_set_params_parse_missing_ex_value() {
    let items = vec![
      Value::BulkString(Some(b"SET".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
      Value::BulkString(Some(b"EX".to_vec())),
    ];
    assert!(SetParams::parse(&items).is_none());
  }

  #[test]
  fn test_set_params_parse_wrong_args() {
    let items = vec![
      Value::BulkString(Some(b"SET".to_vec())),
      Value::BulkString(Some(b"key".to_vec())),
    ];
    assert!(SetParams::parse(&items).is_none());
  }

  #[test]
  fn test_set_params_parse_multiple_options() {
    // Test NX + GET + EX combination
    let items = vec![
      Value::BulkString(Some(b"SET".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
      Value::BulkString(Some(b"GET".to_vec())),
      Value::BulkString(Some(b"NX".to_vec())),
      Value::BulkString(Some(b"EX".to_vec())),
      Value::BulkString(Some(b"120".to_vec())),
    ];
    let params = SetParams::parse(&items).unwrap();
    assert_eq!(params.mode, Some(SetMode::Nx));
    assert_eq!(params.get, true);
    assert_eq!(params.expiration, Some(Expiration::Ex(120)));

    // Test XX + GET + PX combination
    let items = vec![
      Value::BulkString(Some(b"SET".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
      Value::BulkString(Some(b"XX".to_vec())),
      Value::BulkString(Some(b"GET".to_vec())),
      Value::BulkString(Some(b"PX".to_vec())),
      Value::BulkString(Some(b"5000".to_vec())),
    ];
    let params = SetParams::parse(&items).unwrap();
    assert_eq!(params.mode, Some(SetMode::Xx));
    assert_eq!(params.get, true);
    assert_eq!(params.expiration, Some(Expiration::Px(5000)));
  }

  #[test]
  fn test_set_params_parse_invalid_expiration_combination() {
    // Cannot combine different expiration options
    let items = vec![
      Value::BulkString(Some(b"SET".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
      Value::BulkString(Some(b"EX".to_vec())),
      Value::BulkString(Some(b"60".to_vec())),
      Value::BulkString(Some(b"PX".to_vec())),
      Value::BulkString(Some(b"1000".to_vec())),
    ];
    assert!(SetParams::parse(&items).is_none());

    // Cannot combine EX with KEEPTTL
    let items = vec![
      Value::BulkString(Some(b"SET".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
      Value::BulkString(Some(b"KEEPTTL".to_vec())),
      Value::BulkString(Some(b"EX".to_vec())),
      Value::BulkString(Some(b"60".to_vec())),
    ];
    assert!(SetParams::parse(&items).is_none());
  }
}
