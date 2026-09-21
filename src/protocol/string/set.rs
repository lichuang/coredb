use crate::encoding::{NO_EXPIRATION, StringValue};
use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::{MAX_CAS_RETRIES, now_ms};
use async_trait::async_trait;
use rockraft::raft::types::{TxnCondition, TxnReply, TxnReq, UpsertKV};

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
#[derive(Debug, Clone, Copy, PartialEq)]
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
  fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
    // Minimum: SET key value
    if items.len() < 3 {
      return Err(ProtocolError::WrongArgCount("set"));
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

    let mut params = SetParams::new(key, value);
    let mut i = 3;

    // Parse optional arguments
    while i < items.len() {
      let arg = match &items[i] {
        Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_uppercase(),
        Value::SimpleString(s) => s.to_uppercase(),
        _ => return Err(ProtocolError::SyntaxError),
      };

      match arg.as_str() {
        "NX" => {
          if params.mode.is_some() {
            return Err(ProtocolError::SyntaxError);
          }
          params.mode = Some(SetMode::Nx);
          i += 1;
        }
        "XX" => {
          if params.mode.is_some() {
            return Err(ProtocolError::SyntaxError);
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
            return Err(ProtocolError::SyntaxError);
          }
          let seconds = parse_u64(&items[i + 1])?;
          params.expiration = Some(Expiration::Ex(seconds));
          i += 2;
        }
        "PX" => {
          if params.expiration.is_some() || i + 1 >= items.len() {
            return Err(ProtocolError::SyntaxError);
          }
          let milliseconds = parse_u64(&items[i + 1])?;
          params.expiration = Some(Expiration::Px(milliseconds));
          i += 2;
        }
        "EXAT" => {
          if params.expiration.is_some() || i + 1 >= items.len() {
            return Err(ProtocolError::SyntaxError);
          }
          let timestamp = parse_u64(&items[i + 1])?;
          params.expiration = Some(Expiration::ExAt(timestamp));
          i += 2;
        }
        "PXAT" => {
          if params.expiration.is_some() || i + 1 >= items.len() {
            return Err(ProtocolError::SyntaxError);
          }
          let timestamp = parse_u64(&items[i + 1])?;
          params.expiration = Some(Expiration::PxAt(timestamp));
          i += 2;
        }
        "KEEPTTL" => {
          if params.expiration.is_some() {
            return Err(ProtocolError::SyntaxError);
          }
          params.expiration = Some(Expiration::KeepTTL);
          i += 1;
        }
        _ => return Err(ProtocolError::SyntaxError),
      }
    }

    Ok(params)
  }
}

/// Parse a Value as u64
fn parse_u64(value: &Value) -> Result<u64, ProtocolError> {
  match value {
    Value::BulkString(Some(data)) => String::from_utf8_lossy(data)
      .parse::<u64>()
      .map_err(|_| ProtocolError::NotAnInteger),
    Value::SimpleString(s) => s.parse::<u64>().map_err(|_| ProtocolError::NotAnInteger),
    Value::Integer(i) if *i >= 0 => Ok(*i as u64),
    _ => Err(ProtocolError::NotAnInteger),
  }
}

/// SET command executor
pub struct SetCommand;

#[async_trait]
impl Command for SetCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    let params = SetParams::parse(items)?;

    let now = now_ms();
    let new_value = params.value.clone();
    let with_get = params.get;

    match params.mode {
      // Unconditional SET: one atomic log entry, no condition needed. GET's
      // previous value comes back from the txn.
      None => {
        let expires_at = resolve_expiration(server, &params, now).await?;
        let serialized = build_value(new_value, expires_at);
        if with_get {
          let req = TxnReq::new(vec![]).if_then(UpsertKV::insert(&params.key, &serialized));
          match server.txn(req.with_return_previous()).await? {
            TxnReply::Success { prev_values, .. } => Ok(Value::BulkString(decode_previous(
              prev_values.into_iter().next().flatten(),
              now,
            ))),
          }
        } else {
          server.set(params.key, serialized).await?;
          Ok(Value::ok())
        }
      }
      Some(mode) => {
        let key = params.key.clone();
        let expires_at = resolve_expiration(server, &params, now).await?;
        let serialized = build_value(new_value, expires_at);
        conditional_set(server, &key, serialized, mode, with_get).await
      }
    }
  }
}

async fn resolve_expiration(
  server: &Server,
  params: &SetParams,
  now: u64,
) -> Result<u64, CoreDbError> {
  match &params.expiration {
    Some(Expiration::KeepTTL) => keep_ttl_expires_at(server, &params.key, now).await,
    _ => Ok(absolute_expires_at(&params.expiration, now)),
  }
}

fn absolute_expires_at(expiration: &Option<Expiration>, now: u64) -> u64 {
  match expiration {
    Some(Expiration::Ex(seconds)) => now + seconds * 1000,
    Some(Expiration::Px(millis)) => now + millis,
    Some(Expiration::ExAt(timestamp)) => timestamp * 1000,
    Some(Expiration::PxAt(timestamp)) => *timestamp,
    _ => NO_EXPIRATION,
  }
}

async fn keep_ttl_expires_at(server: &Server, key: &str, now: u64) -> Result<u64, CoreDbError> {
  Ok(match server.get(key).await? {
    Some(raw_value) => match StringValue::deserialize(&raw_value) {
      Ok(existing) if existing.is_expired(now) => NO_EXPIRATION,
      Ok(existing) if existing.has_expiration() => existing.expires_at,
      _ => NO_EXPIRATION,
    },
    None => NO_EXPIRATION,
  })
}

fn build_value(data: Vec<u8>, expires_at: u64) -> Vec<u8> {
  if expires_at == NO_EXPIRATION {
    StringValue::new(data)
  } else {
    StringValue::with_expiration(data, expires_at)
  }
  .serialize()
}

/// Decode a previous-value returned by the transaction into user data.
///
/// rockraft returns the raw stored bytes (header + payload); the GET option
/// must reply with the decoded string, and nil for an expired or foreign-type
/// value.
fn decode_previous(raw: Option<Vec<u8>>, now: u64) -> Option<Vec<u8>> {
  let bytes = raw?;
  match StringValue::deserialize(&bytes) {
    Ok(sv) if !sv.is_expired(now) => Some(sv.data),
    _ => None,
  }
}

/// Observed state of the key, before any write.
enum Observed {
  Absent,
  /// A live (unexpired) string.
  Live,
  /// Bytes exist but belong to an expired string: logically absent, but the
  /// CAS must still pin these exact bytes.
  Expired,
  /// A key of some other type; SET overwrites it.
  OtherType,
}

/// The reply for a conditional SET: whether the value landed, and the previous
/// value when the GET option was requested.
struct SetOutcome {
  applied: bool,
  previous: Option<Vec<u8>>,
}

/// SETNX semantics shared with the SETNX command: insert only if the key does
/// not exist, atomically. Returns whether the value was applied.
pub async fn set_if_not_exists(
  server: &Server,
  key: &str,
  serialized: Vec<u8>,
) -> Result<bool, CoreDbError> {
  for _ in 0..MAX_CAS_RETRIES {
    let raw = server.get(key).await?;
    let observed = classify(&raw, now_ms());

    if matches!(observed, Observed::Live | Observed::OtherType) {
      return Ok(false);
    }

    // Pin the observed state: `not_exists` when absent, the exact bytes when an
    // expired string is being resurrected.
    let condition = match &raw {
      None => TxnCondition::not_exists(key),
      Some(bytes) => TxnCondition::eq(key, bytes),
    };

    let req = TxnReq::new(vec![condition]).if_then(UpsertKV::insert(key, &serialized));
    match server.txn(req).await? {
      TxnReply::Success { branch: true, .. } => return Ok(true),
      _ => continue,
    }
  }

  Err(ProtocolError::Custom("ERR set retry limit exceeded").into())
}

/// Apply `SET key value [NX|XX]` atomically.
///
/// The observation and the write are two separate Raft round trips, so the
/// write is guarded by a condition that pins the observed bytes: the insert
/// only lands if the key is still in the observed state at apply time. Losing
/// the race re-observes and retries, so the decision is always made against
/// the state at apply time — two racing `SET NX` on an absent key can no
/// longer both succeed.
async fn conditional_set(
  server: &Server,
  key: &str,
  serialized: Vec<u8>,
  mode: SetMode,
  with_get: bool,
) -> Result<Value, CoreDbError> {
  let now = now_ms();

  for _ in 0..MAX_CAS_RETRIES {
    let raw = server.get(key).await?;
    let observed = classify(&raw, now);

    if let Some(outcome) = rejected_by_mode(&observed, mode, &raw, with_get) {
      return Ok(outcome);
    }

    let condition = match &raw {
      None => TxnCondition::not_exists(key),
      Some(bytes) => TxnCondition::eq(key, bytes),
    };

    let mut req = TxnReq::new(vec![condition]).if_then(UpsertKV::insert(key, &serialized));
    if with_get {
      req = req.with_return_previous();
    }

    match server.txn(req).await? {
      TxnReply::Success {
        branch: true,
        prev_values,
      } => {
        let previous = if with_get {
          decode_previous(prev_values.into_iter().next().flatten(), now)
        } else {
          None
        };
        return Ok(make_reply(
          SetOutcome {
            applied: true,
            previous,
          },
          with_get,
        ));
      }
      _ => continue,
    }
  }

  Err(ProtocolError::Custom("ERR set retry limit exceeded").into())
}

fn classify(raw: &Option<Vec<u8>>, now: u64) -> Observed {
  match raw {
    None => Observed::Absent,
    Some(bytes) => match StringValue::deserialize(bytes) {
      Ok(sv) if sv.is_expired(now) => Observed::Expired,
      Ok(_) => Observed::Live,
      Err(_) => Observed::OtherType,
    },
  }
}

/// Reject without writing when the observed state already decides the outcome.
///
/// These paths mirror the unconditional read-then-decide behaviour: the residual
/// race (the key changing between observation and reply) is the same one the
/// pre-transaction code had, and avoiding a write here keeps failed NX/XX
/// attempts cheap.
fn rejected_by_mode(
  observed: &Observed,
  mode: SetMode,
  raw: &Option<Vec<u8>>,
  with_get: bool,
) -> Option<Value> {
  let rejected = match mode {
    SetMode::Nx => matches!(observed, Observed::Live | Observed::OtherType),
    SetMode::Xx => matches!(observed, Observed::Absent | Observed::Expired),
  };
  if !rejected {
    return None;
  }
  let previous = if with_get {
    observed_previous(observed, raw)
  } else {
    None
  };
  Some(make_reply(
    SetOutcome {
      applied: false,
      previous,
    },
    with_get,
  ))
}

fn observed_previous(observed: &Observed, raw: &Option<Vec<u8>>) -> Option<Vec<u8>> {
  match observed {
    Observed::Live => raw
      .as_deref()
      .and_then(|bytes| StringValue::deserialize(bytes).ok())
      .map(|sv| sv.data),
    _ => None,
  }
}

fn make_reply(outcome: SetOutcome, with_get: bool) -> Value {
  if with_get {
    Value::BulkString(outcome.previous)
  } else if outcome.applied {
    Value::ok()
  } else {
    Value::BulkString(None)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_classify_states() {
    let now = 10_000;

    assert!(matches!(classify(&None, now), Observed::Absent));

    let live = StringValue::with_expiration(b"v", 20_000).serialize();
    assert!(matches!(classify(&Some(live), now), Observed::Live));

    let expired = StringValue::with_expiration(b"v", 5_000).serialize();
    assert!(matches!(classify(&Some(expired), now), Observed::Expired));

    let no_ttl = StringValue::new(b"v").serialize();
    assert!(matches!(classify(&Some(no_ttl), now), Observed::Live));

    // Other type (e.g. hash metadata bytes) decodes as OtherType.
    let other = b"\x12\x00\x00\x00\x00\x00\x00\x00\x00\x00".to_vec();
    assert!(matches!(classify(&Some(other), now), Observed::OtherType));
  }

  /// NX/XX fast-reject decisions, per observed state.
  #[test]
  fn test_rejected_by_mode_matrix() {
    let now = 10_000;
    let live = StringValue::with_expiration(b"v", 20_000).serialize();
    let expired = StringValue::with_expiration(b"v", 5_000).serialize();

    let cases = [
      (None, SetMode::Nx, false),
      (None, SetMode::Xx, true),
      (Some(live.clone()), SetMode::Nx, true),
      (Some(live.clone()), SetMode::Xx, false),
      (Some(expired.clone()), SetMode::Nx, false),
      (Some(expired), SetMode::Xx, true),
    ];

    for (raw, mode, expect_reject) in cases {
      let observed = classify(&raw, now);
      let rejected = rejected_by_mode(&observed, mode, &raw, false);
      assert_eq!(
        rejected.is_some(),
        expect_reject,
        "raw={raw:?} mode={mode:?}"
      );
      if let Some(reply) = rejected {
        assert_eq!(
          reply,
          Value::BulkString(None),
          "rejected reply must be nil for {mode:?}"
        );
      }
    }
  }

  #[test]
  fn test_make_reply_applied_vs_rejected() {
    assert_eq!(
      make_reply(
        SetOutcome {
          applied: true,
          previous: None
        },
        false
      ),
      Value::ok()
    );
    assert_eq!(
      make_reply(
        SetOutcome {
          applied: false,
          previous: None
        },
        false
      ),
      Value::BulkString(None)
    );
    assert_eq!(
      make_reply(
        SetOutcome {
          applied: true,
          previous: Some(b"old".to_vec())
        },
        true
      ),
      Value::BulkString(Some(b"old".to_vec()))
    );
  }

  #[test]
  fn test_observed_previous_only_for_live_strings() {
    let now = 10_000;
    let live = StringValue::with_expiration(b"old-value", 20_000).serialize();
    assert_eq!(
      observed_previous(&Observed::Live, &Some(live)),
      Some(b"old-value".to_vec())
    );
    assert_eq!(observed_previous(&Observed::Absent, &None), None);
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
    assert!(SetParams::parse(&items).is_err());
  }

  #[test]
  fn test_set_params_parse_missing_ex_value() {
    let items = vec![
      Value::BulkString(Some(b"SET".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
      Value::BulkString(Some(b"EX".to_vec())),
    ];
    assert!(SetParams::parse(&items).is_err());
  }

  #[test]
  fn test_set_params_parse_wrong_args() {
    let items = vec![
      Value::BulkString(Some(b"SET".to_vec())),
      Value::BulkString(Some(b"key".to_vec())),
    ];
    assert!(SetParams::parse(&items).is_err());
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
    assert!(SetParams::parse(&items).is_err());

    // Cannot combine EX with KEEPTTL
    let items = vec![
      Value::BulkString(Some(b"SET".to_vec())),
      Value::BulkString(Some(b"mykey".to_vec())),
      Value::BulkString(Some(b"myvalue".to_vec())),
      Value::BulkString(Some(b"KEEPTTL".to_vec())),
      Value::BulkString(Some(b"EX".to_vec())),
      Value::BulkString(Some(b"60".to_vec())),
    ];
    assert!(SetParams::parse(&items).is_err());
  }
}
