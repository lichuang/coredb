//! ZADD command implementation
//!
//! ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]
//! Adds all the specified members with the specified scores to the sorted
//! set stored at key.
//!
//! Concurrency: the member sub-key space is qualified by the metadata's
//! `version`, which is a millisecond timestamp regenerated when a key is
//! (re)created. Concurrent ZADDs that read the same metadata would each
//! write `size = read_size + 1` back — losing members from the count — and
//! worse, writers landing on different milliseconds would file members into
//! different version spaces while only the last metadata survives, making
//! those members invisible to scans (docs/bug.md §1.4). The read-modify-write
//! is therefore guarded by a conditional transaction that pins the observed
//! metadata bytes; losers re-read and retry with capped, jittered backoff,
//! which also serializes version assignment.

use rockraft::raft::types::{TxnCondition, TxnReply, TxnReq, UpsertKV};

use crate::encoding::{TYPE_ZSET, ZSetMemberValue, ZSetMetadata};
use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::{MAX_CAS_RETRIES, backoff_delay, now_ms};
use async_trait::async_trait;

struct ZAddArgs {
  key: String,
  nx: bool,
  xx: bool,
  gt: bool,
  lt: bool,
  #[allow(dead_code)]
  ch: bool,
  members: Vec<(Vec<u8>, f64)>,
}

pub struct ZAddCommand;

impl ZAddCommand {
  fn parse_args(items: &[Value]) -> Result<ZAddArgs, ProtocolError> {
    // Minimum: ZADD key score member (4 items)
    if items.len() < 4 {
      return Err(ProtocolError::WrongArgCount("zadd"));
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::InvalidArgument("key")),
    };

    let mut nx = false;
    let mut xx = false;
    let mut gt = false;
    let mut lt = false;
    let mut ch = false;

    // Parse flags from items[2..] until we hit a score (number)
    let mut i = 2;
    while i < items.len() {
      let flag = match &items[i] {
        Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
        Value::SimpleString(s) => s.clone(),
        _ => break,
      };

      match flag.to_uppercase().as_str() {
        "NX" => {
          nx = true;
          i += 1;
        }
        "XX" => {
          xx = true;
          i += 1;
        }
        "GT" => {
          gt = true;
          i += 1;
        }
        "LT" => {
          lt = true;
          i += 1;
        }
        "CH" => {
          ch = true;
          i += 1;
        }
        _ => break,
      }
    }

    if nx && xx {
      return Err(ProtocolError::Custom(
        "ERR XX and NX options at the same time are not compatible",
      ));
    }

    if gt && lt {
      return Err(ProtocolError::Custom(
        "ERR GT and LT options at the same time are not compatible",
      ));
    }

    // Remaining items must be score-member pairs
    let remaining = &items[i..];
    if remaining.is_empty() || !remaining.len().is_multiple_of(2) {
      return Err(ProtocolError::WrongArgCount("zadd"));
    }

    let mut members = Vec::with_capacity(remaining.len() / 2);
    let mut j = 0;
    while j < remaining.len() {
      let score = match &remaining[j] {
        Value::BulkString(Some(data)) => {
          let s = String::from_utf8_lossy(data);
          match s.parse::<f64>() {
            Ok(v) => v,
            Err(_) => return Err(ProtocolError::Custom("ERR value is not a valid float")),
          }
        }
        Value::SimpleString(s) => match s.parse::<f64>() {
          Ok(v) => v,
          Err(_) => return Err(ProtocolError::Custom("ERR value is not a valid float")),
        },
        _ => return Err(ProtocolError::Custom("ERR value is not a valid float")),
      };

      let member = match &remaining[j + 1] {
        Value::BulkString(Some(data)) => data.clone(),
        Value::SimpleString(s) => s.as_bytes().to_vec(),
        _ => return Err(ProtocolError::InvalidArgument("member")),
      };

      members.push((member, score));
      j += 2;
    }

    Ok(ZAddArgs {
      key,
      nx,
      xx,
      gt,
      lt,
      ch,
      members,
    })
  }

  #[allow(dead_code)]
  fn parse_score(data: &[u8]) -> Option<f64> {
    let s = String::from_utf8_lossy(data);
    s.parse::<f64>().ok()
  }
}

/// The current zset state observed for the target key.
enum CurrentZSet {
  /// Key absent: push starts a fresh zset, guarded by `not_exists`.
  Absent,
  /// Present, unexpired, and a zset; carries its serialized metadata bytes.
  ZSet {
    raw: Vec<u8>,
    metadata: ZSetMetadata,
  },
  /// Present but expired: logically absent, yet its bytes still sit in the
  /// store, so the CAS must pin those bytes with `eq` — `not_exists` would
  /// never hold and the command would spin out its retry budget. The fresh
  /// zset gets a new version, so old member sub-keys become invisible.
  Expired { raw: Vec<u8> },
  /// Present but not a zset value: report WRONGTYPE instead of overwriting.
  Other,
}

async fn observe_current(server: &Server, key: &str) -> Result<CurrentZSet, CoreDbError> {
  let raw = match server.get(key).await? {
    Some(bytes) => bytes,
    None => return Ok(CurrentZSet::Absent),
  };

  match ZSetMetadata::deserialize(&raw) {
    Ok(meta) if meta.get_type() != TYPE_ZSET => Ok(CurrentZSet::Other),
    Ok(meta) if meta.is_expired(now_ms()) => Ok(CurrentZSet::Expired { raw }),
    Ok(meta) => Ok(CurrentZSet::ZSet {
      raw,
      metadata: meta,
    }),
    Err(_) => Ok(CurrentZSet::Other),
  }
}

#[async_trait]
impl Command for ZAddCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    let args = Self::parse_args(items)?;

    for attempt in 0..MAX_CAS_RETRIES {
      let (condition, mut metadata) = match observe_current(server, &args.key).await? {
        CurrentZSet::Other => return Err(ProtocolError::WrongType.into()),
        CurrentZSet::Absent => (TxnCondition::not_exists(&args.key), ZSetMetadata::new()),
        // An expired zset reads as absent, but its bytes are still stored:
        // pin them with `eq` (not_exists would never hold). The fresh zset
        // carries a new version, so expired members stay unreachable.
        CurrentZSet::Expired { raw } => {
          let metadata = ZSetMetadata::new();
          (TxnCondition::eq(&args.key, raw), metadata)
        }
        CurrentZSet::ZSet { raw, metadata } => (TxnCondition::eq(&args.key, raw), metadata),
      };

      let version = metadata.version;
      let mut added_count = 0i64;
      let mut updated_count = 0i64;
      let mut entries: Vec<UpsertKV> = Vec::with_capacity(args.members.len() + 1);

      for (member, score) in &args.members {
        let sub_key_str = ZSetMemberValue::build_sub_key_hex(args.key.as_bytes(), version, member);

        let member_exists = match server.get(&sub_key_str).await? {
          Some(raw_val) => match ZSetMemberValue::deserialize(&raw_val) {
            Ok(existing) => {
              // NX: skip if already exists
              if args.nx {
                continue;
              }

              let score_changed = if args.gt {
                *score > existing.score
              } else if args.lt {
                *score < existing.score
              } else {
                (existing.score - score).abs() > f64::EPSILON
              };

              if score_changed {
                entries.push(UpsertKV::insert(
                  sub_key_str.clone(),
                  &ZSetMemberValue::new(*score).serialize(),
                ));
                updated_count += 1;
              }
              true
            }
            Err(_) => false,
          },
          None => false,
        };

        if !member_exists {
          // XX: skip if does not exist
          if args.xx {
            continue;
          }

          entries.push(UpsertKV::insert(
            sub_key_str,
            &ZSetMemberValue::new(*score).serialize(),
          ));
          metadata.incr_size();
          added_count += 1;
        }
      }

      entries.push(UpsertKV::insert(args.key.clone(), &metadata.serialize()));

      // The condition pins the observed metadata bytes, which qualify the
      // member sub-key space: the transaction only applies if nothing else
      // has touched the zset, so version assignment is serialized.
      let reply = server
        .txn(TxnReq::new(vec![condition]).if_then_ops(entries))
        .await?;

      match reply {
        TxnReply::Success { branch: true, .. } => {
          return Ok(if args.ch {
            Value::Integer(added_count + updated_count)
          } else {
            Value::Integer(added_count)
          });
        }
        // Lost the race; back off (jittered, capped) so concurrent losers do
        // not re-compete in lockstep, then re-observe.
        _ => {
          tokio::time::sleep(backoff_delay(attempt)).await;
          continue;
        }
      }
    }

    Err(ProtocolError::Custom("ERR zadd retry limit exceeded").into())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn bulk(data: &[u8]) -> Value {
    Value::BulkString(Some(data.to_vec()))
  }

  fn ss(s: &str) -> Value {
    Value::SimpleString(s.to_string())
  }

  #[test]
  fn test_parse_args_basic() {
    let items = vec![ss("ZADD"), bulk(b"myzset"), ss("1.0"), bulk(b"member1")];

    let args = ZAddCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "myzset");
    assert!(!args.nx);
    assert!(!args.xx);
    assert!(!args.ch);
    assert_eq!(args.members.len(), 1);
    assert_eq!(args.members[0].0, b"member1");
    assert!((args.members[0].1 - 1.0).abs() < f64::EPSILON);
  }

  #[test]
  fn test_parse_args_multiple_pairs() {
    let items = vec![
      ss("ZADD"),
      bulk(b"myzset"),
      ss("1.0"),
      bulk(b"a"),
      ss("2.0"),
      bulk(b"b"),
      ss("3.0"),
      bulk(b"c"),
    ];

    let args = ZAddCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "myzset");
    assert_eq!(args.members.len(), 3);
    assert!((args.members[0].1 - 1.0).abs() < f64::EPSILON);
    assert!((args.members[1].1 - 2.0).abs() < f64::EPSILON);
    assert!((args.members[2].1 - 3.0).abs() < f64::EPSILON);
  }

  #[test]
  fn test_parse_args_nx() {
    let items = vec![
      ss("ZADD"),
      bulk(b"myzset"),
      ss("NX"),
      ss("1.0"),
      bulk(b"member1"),
    ];

    let args = ZAddCommand::parse_args(&items).unwrap();
    assert!(args.nx);
    assert!(!args.xx);
    assert_eq!(args.members.len(), 1);
  }

  #[test]
  fn test_parse_args_xx() {
    let items = vec![
      ss("ZADD"),
      bulk(b"myzset"),
      ss("XX"),
      ss("1.0"),
      bulk(b"member1"),
    ];

    let args = ZAddCommand::parse_args(&items).unwrap();
    assert!(args.xx);
    assert!(!args.nx);
  }

  #[test]
  fn test_parse_args_ch() {
    let items = vec![
      ss("ZADD"),
      bulk(b"myzset"),
      ss("CH"),
      ss("1.0"),
      bulk(b"member1"),
    ];

    let args = ZAddCommand::parse_args(&items).unwrap();
    assert!(args.ch);
  }

  #[test]
  fn test_parse_args_gt() {
    let items = vec![
      ss("ZADD"),
      bulk(b"myzset"),
      ss("GT"),
      ss("10.0"),
      bulk(b"member1"),
    ];

    let args = ZAddCommand::parse_args(&items).unwrap();
    assert!(args.gt);
  }

  #[test]
  fn test_parse_args_lt() {
    let items = vec![
      ss("ZADD"),
      bulk(b"myzset"),
      ss("LT"),
      ss("10.0"),
      bulk(b"member1"),
    ];

    let args = ZAddCommand::parse_args(&items).unwrap();
    assert!(args.lt);
  }

  #[test]
  fn test_parse_args_nx_xx_conflict() {
    let items = vec![
      ss("ZADD"),
      bulk(b"myzset"),
      ss("NX"),
      ss("XX"),
      ss("1.0"),
      bulk(b"member1"),
    ];

    let result = ZAddCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_gt_lt_conflict() {
    let items = vec![
      ss("ZADD"),
      bulk(b"myzset"),
      ss("GT"),
      ss("LT"),
      ss("1.0"),
      bulk(b"member1"),
    ];

    let result = ZAddCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_multiple_flags() {
    let items = vec![
      ss("ZADD"),
      bulk(b"myzset"),
      ss("NX"),
      ss("CH"),
      ss("1.0"),
      bulk(b"member1"),
    ];

    let args = ZAddCommand::parse_args(&items).unwrap();
    assert!(args.nx);
    assert!(args.ch);
  }

  #[test]
  fn test_parse_args_negative_score() {
    let items = vec![ss("ZADD"), bulk(b"myzset"), ss("-10.5"), bulk(b"member1")];

    let args = ZAddCommand::parse_args(&items).unwrap();
    assert!((args.members[0].1 - (-10.5)).abs() < f64::EPSILON);
  }

  #[test]
  fn test_parse_args_insufficient() {
    let items = vec![ss("ZADD"), bulk(b"myzset")];
    let result = ZAddCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_no_args() {
    let items = vec![ss("ZADD")];
    let result = ZAddCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_missing_member() {
    let items = vec![ss("ZADD"), bulk(b"myzset"), ss("1.0")];
    let result = ZAddCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_invalid_score() {
    let items = vec![
      ss("ZADD"),
      bulk(b"myzset"),
      ss("not_a_number"),
      bulk(b"member1"),
    ];
    let result = ZAddCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_invalid_key_type() {
    let items = vec![ss("ZADD"), Value::Integer(42), ss("1.0"), bulk(b"member1")];
    let result = ZAddCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_invalid_member_type() {
    let items = vec![ss("ZADD"), bulk(b"myzset"), ss("1.0"), Value::Integer(42)];
    let result = ZAddCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_score_as_bulk_string() {
    let items = vec![ss("ZADD"), bulk(b"myzset"), bulk(b"3.14"), bulk(b"member1")];

    let args = ZAddCommand::parse_args(&items).unwrap();
    assert!((args.members[0].1 - 3.14).abs() < f64::EPSILON);
  }

  #[test]
  fn test_parse_args_flags_case_insensitive() {
    let items = vec![
      ss("ZADD"),
      bulk(b"myzset"),
      ss("nx"),
      ss("ch"),
      ss("1.0"),
      bulk(b"member1"),
    ];

    let args = ZAddCommand::parse_args(&items).unwrap();
    assert!(args.nx);
    assert!(args.ch);
  }
}
