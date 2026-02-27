use crate::encoding::StringValue;
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
  KeepTtl,
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
          params.expiration = Some(Expiration::KeepTtl);
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

    // Calculate expiration timestamp in milliseconds
    let expires_at = params.expiration.and_then(|exp| {
      let now = now_ms();
      match exp {
        Expiration::Ex(seconds) => Some(now + seconds * 1000),
        Expiration::Px(millis) => Some(now + millis),
        Expiration::ExAt(timestamp) => Some(timestamp * 1000),
        Expiration::PxAt(timestamp) => Some(timestamp),
        Expiration::KeepTtl => None, // TODO: Implement KEEPTTL
      }
    });

    // Create StringValue and serialize
    let string_value = match expires_at {
      Some(exp) => StringValue::with_expiration(params.value, exp),
      None => StringValue::new(params.value),
    };
    let serialized = string_value.serialize();

    // TODO: Implement NX/XX mode logic
    // TODO: Implement GET option to return previous value
    // TODO: Implement KEEPTTL for expiration

    // For now, just set the value (basic implementation)
    match server.set(params.key, serialized).await {
      Ok(_) => {
        if params.get {
          // TODO: Return previous value when GET option is implemented
          Value::BulkString(None)
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
  use crate::server::Server;
  use std::sync::Arc;

  #[tokio::test]
  async fn test_set_and_get_with_expiration() {
    let server = Arc::new(Server::bind("127.0.0.1:0").await.unwrap());

    // Set key with 1 second expiration
    let set_items = vec![
      Value::BulkString(Some(b"SET".to_vec())),
      Value::BulkString(Some(b"test_key".to_vec())),
      Value::BulkString(Some(b"test_value".to_vec())),
      Value::BulkString(Some(b"PX".to_vec())),
      Value::BulkString(Some(b"100".to_vec())), // 100ms expiration
    ];

    let set_cmd = SetCommand;
    let result = set_cmd.execute(&set_items, &server).await;
    assert_eq!(result, Value::ok());

    // Get should return the value immediately
    let get_items = vec![
      Value::BulkString(Some(b"GET".to_vec())),
      Value::BulkString(Some(b"test_key".to_vec())),
    ];

    let get_cmd = crate::protocol::get::GetCommand;
    let result = get_cmd.execute(&get_items, &server).await;
    assert_eq!(result, Value::BulkString(Some(b"test_value".to_vec())));

    // Wait for expiration
    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

    // Get should return null after expiration
    let result = get_cmd.execute(&get_items, &server).await;
    assert_eq!(result, Value::BulkString(None));
  }

  #[tokio::test]
  async fn test_set_without_expiration() {
    let server = Arc::new(Server::bind("127.0.0.1:0").await.unwrap());

    // Set key without expiration
    let set_items = vec![
      Value::BulkString(Some(b"SET".to_vec())),
      Value::BulkString(Some(b"persistent_key".to_vec())),
      Value::BulkString(Some(b"persistent_value".to_vec())),
    ];

    let set_cmd = SetCommand;
    let result = set_cmd.execute(&set_items, &server).await;
    assert_eq!(result, Value::ok());

    // Get should return the value
    let get_items = vec![
      Value::BulkString(Some(b"GET".to_vec())),
      Value::BulkString(Some(b"persistent_key".to_vec())),
    ];

    let get_cmd = crate::protocol::get::GetCommand;
    let result = get_cmd.execute(&get_items, &server).await;
    assert_eq!(
      result,
      Value::BulkString(Some(b"persistent_value".to_vec()))
    );
  }

  #[tokio::test]
  async fn test_get_nonexistent_key() {
    let server = Arc::new(Server::bind("127.0.0.1:0").await.unwrap());

    let get_items = vec![
      Value::BulkString(Some(b"GET".to_vec())),
      Value::BulkString(Some(b"nonexistent_key".to_vec())),
    ];

    let get_cmd = crate::protocol::get::GetCommand;
    let result = get_cmd.execute(&get_items, &server).await;
    assert_eq!(result, Value::BulkString(None));
  }

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
    assert_eq!(params.expiration, Some(Expiration::KeepTtl));
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
}
