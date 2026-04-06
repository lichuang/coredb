//! ZREVRANGE command implementation
//!
//! ZREVRANGE key start stop [WITHSCORES]
//! Returns the specified range of elements in the sorted set stored at key.
//! Elements are ordered from the highest to the lowest score.
//! Deduplicating members with the same score are ordered lexicographically
//! in reverse order (descending).
//!
//! Start and stop are 0-based indices. Negative indices count from the end.
//! WITHSCORES returns member-score pairs.
//!
//! Return value:
//! - Array of members (or member, score, member, score, ... with WITHSCORES)
//! - Empty array if key does not exist
//! - WRONGTYPE error if key exists but is not a sorted set

use crate::encoding::{TYPE_ZSET, ZSetMemberValue, ZSetMetadata};
use crate::error::{CoreDbError, ProtocolError};
use crate::protocol::command::Command;
use crate::protocol::resp::Value;
use crate::server::Server;
use crate::util::now_ms;
use async_trait::async_trait;

/// ZREVRANGE command handler
pub struct ZRevRangeCommand;

/// Parsed arguments for ZREVRANGE
struct ZRevRangeArgs {
  key: String,
  start: i64,
  stop: i64,
  with_scores: bool,
}

impl ZRevRangeCommand {
  /// Parse ZREVRANGE arguments: ZREVRANGE key start stop [WITHSCORES]
  fn parse_args(items: &[Value]) -> Result<ZRevRangeArgs, ProtocolError> {
    if items.len() < 4 {
      return Err(ProtocolError::WrongArgCount("zrevrange"));
    }

    let key = match &items[1] {
      Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
      Value::SimpleString(s) => s.clone(),
      _ => return Err(ProtocolError::InvalidArgument("key")),
    };

    let start = match &items[2] {
      Value::BulkString(Some(data)) => {
        let s = String::from_utf8_lossy(data);
        s.parse::<i64>().map_err(|_| ProtocolError::NotAnInteger)?
      }
      Value::SimpleString(s) => s.parse::<i64>().map_err(|_| ProtocolError::NotAnInteger)?,
      Value::Integer(n) => *n,
      _ => return Err(ProtocolError::NotAnInteger),
    };

    let stop = match &items[3] {
      Value::BulkString(Some(data)) => {
        let s = String::from_utf8_lossy(data);
        s.parse::<i64>().map_err(|_| ProtocolError::NotAnInteger)?
      }
      Value::SimpleString(s) => s.parse::<i64>().map_err(|_| ProtocolError::NotAnInteger)?,
      Value::Integer(n) => *n,
      _ => return Err(ProtocolError::NotAnInteger),
    };

    // Check for WITHSCORES flag
    let mut with_scores = false;
    if items.len() > 4 {
      for item in &items[4..] {
        let flag = match item {
          Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
          Value::SimpleString(s) => s.clone(),
          _ => continue,
        };
        if flag.to_uppercase() == "WITHSCORES" {
          with_scores = true;
        }
      }
    }

    Ok(ZRevRangeArgs {
      key,
      start,
      stop,
      with_scores,
    })
  }
}

#[async_trait]
impl Command for ZRevRangeCommand {
  async fn execute(&self, items: &[Value], server: &Server) -> Result<Value, CoreDbError> {
    let args = Self::parse_args(items)?;

    // Get metadata
    let metadata = match server.get(&args.key).await? {
      Some(raw_meta) => match ZSetMetadata::deserialize(&raw_meta) {
        Ok(meta) => {
          if meta.get_type() != TYPE_ZSET {
            return Err(ProtocolError::WrongType.into());
          }
          if meta.is_expired(now_ms()) {
            return Ok(Value::Array(Some(vec![])));
          }
          meta
        }
        Err(_) => return Ok(Value::Array(Some(vec![]))),
      },
      None => return Ok(Value::Array(Some(vec![]))),
    };

    let version = metadata.version;

    // Build the hex-encoded prefix for scanning all members of this zset
    let prefix_hex = build_member_prefix_hex(args.key.as_bytes(), version);

    // Scan for all member sub-keys
    let scan_results = server.scan_prefix(prefix_hex.as_bytes()).await?;

    // Parse results into (member, score) pairs
    let mut members: Vec<(Vec<u8>, f64)> = Vec::with_capacity(scan_results.len());

    for (sub_key_hex, sub_value) in scan_results {
      // Decode hex key
      let sub_key = match String::from_utf8(sub_key_hex) {
        Ok(hex_str) => match hex::decode(&hex_str) {
          Ok(bytes) => bytes,
          Err(_) => continue,
        },
        Err(_) => continue,
      };

      // Parse the binary sub_key to extract the member name
      if let Some((_, _, member)) = ZSetMemberValue::parse_sub_key(&sub_key) {
        // Deserialize the score value
        if let Ok(member_value) = ZSetMemberValue::deserialize(&sub_value) {
          members.push((member.to_vec(), member_value.score));
        }
      }
    }

    // Sort by score descending, then by member name descending for equal scores
    members.sort_by(|a, b| {
      b.1
        .partial_cmp(&a.1)
        .unwrap_or(std::cmp::Ordering::Equal)
        .then_with(|| b.0.cmp(&a.0))
    });

    // Convert negative indices to positive
    let len = members.len() as i64;
    let start = if args.start < 0 {
      (len + args.start).max(0)
    } else {
      args.start
    };
    let stop = if args.stop < 0 {
      (len + args.stop).max(-1)
    } else {
      args.stop
    };

    // If start > stop or start > len, return empty array
    if start > stop || start >= len {
      return Ok(Value::Array(Some(vec![])));
    }

    // Clamp stop to len-1
    let stop = stop.min(len - 1);
    let start = start as usize;
    let stop = stop as usize;

    // Build result array
    let mut result = Vec::new();
    for (member, score) in &members[start..=stop] {
      result.push(Value::BulkString(Some(member.clone())));
      if args.with_scores {
        // Redis returns scores as bulk strings (formatted float)
        let score_str = format_score(*score);
        result.push(Value::BulkString(Some(score_str.into_bytes())));
      }
    }

    Ok(Value::Array(Some(result)))
  }
}

/// Format a score as Redis does:
/// - Integer scores: "3" not "3.0"
/// - Float scores: "3.5"
/// - Special: "inf", "-inf", "nan"
fn format_score(score: f64) -> String {
  if score.is_infinite() {
    if score.is_sign_positive() {
      "inf".to_string()
    } else {
      "-inf".to_string()
    }
  } else if score.is_nan() {
    "nan".to_string()
  } else if score.fract() == 0.0 {
    format!("{}", score as i64)
  } else {
    // Redis uses up to 17 significant digits
    let s = format!("{:.17}", score);
    // Remove trailing zeros after decimal point, but keep at least one digit
    let s = s.trim_end_matches('0');
    // Remove trailing dot if all decimals were zeros (shouldn't happen due to fract check)
    s.trim_end_matches('.').to_string()
  }
}

/// Build the hex-encoded prefix for scanning zset members
/// Format: hex(key_len(4 bytes) | key | version(8 bytes))
fn build_member_prefix_hex(key: &[u8], version: u64) -> String {
  let key_len = key.len() as u32;
  let mut prefix = Vec::with_capacity(4 + key.len() + 8);
  prefix.extend_from_slice(&key_len.to_be_bytes());
  prefix.extend_from_slice(key);
  prefix.extend_from_slice(&version.to_be_bytes());
  hex::encode(&prefix)
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
    let items = vec![ss("ZREVRANGE"), bulk(b"myzset"), ss("0"), bulk(b"-1")];
    let args = ZRevRangeCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "myzset");
    assert_eq!(args.start, 0);
    assert_eq!(args.stop, -1);
    assert!(!args.with_scores);
  }

  #[test]
  fn test_parse_args_with_scores() {
    let items = vec![
      ss("ZREVRANGE"),
      bulk(b"myzset"),
      ss("0"),
      bulk(b"-1"),
      ss("WITHSCORES"),
    ];
    let args = ZRevRangeCommand::parse_args(&items).unwrap();
    assert_eq!(args.key, "myzset");
    assert_eq!(args.start, 0);
    assert_eq!(args.stop, -1);
    assert!(args.with_scores);
  }

  #[test]
  fn test_parse_args_with_scores_case_insensitive() {
    let items = vec![
      ss("ZREVRANGE"),
      bulk(b"myzset"),
      ss("0"),
      bulk(b"-1"),
      ss("withscores"),
    ];
    let args = ZRevRangeCommand::parse_args(&items).unwrap();
    assert!(args.with_scores);
  }

  #[test]
  fn test_parse_args_positive_indices() {
    let items = vec![ss("ZREVRANGE"), bulk(b"myzset"), ss("1"), bulk(b"3")];
    let args = ZRevRangeCommand::parse_args(&items).unwrap();
    assert_eq!(args.start, 1);
    assert_eq!(args.stop, 3);
  }

  #[test]
  fn test_parse_args_negative_indices() {
    let items = vec![ss("ZREVRANGE"), bulk(b"myzset"), ss("-3"), bulk(b"-1")];
    let args = ZRevRangeCommand::parse_args(&items).unwrap();
    assert_eq!(args.start, -3);
    assert_eq!(args.stop, -1);
  }

  #[test]
  fn test_parse_args_integer_value_type() {
    let items = vec![
      ss("ZREVRANGE"),
      bulk(b"myzset"),
      Value::Integer(0),
      Value::Integer(-1),
    ];
    let args = ZRevRangeCommand::parse_args(&items).unwrap();
    assert_eq!(args.start, 0);
    assert_eq!(args.stop, -1);
  }

  #[test]
  fn test_parse_args_insufficient() {
    let items = vec![ss("ZREVRANGE"), bulk(b"myzset")];
    let result = ZRevRangeCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_no_args() {
    let items = vec![ss("ZREVRANGE")];
    let result = ZRevRangeCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_invalid_start() {
    let items = vec![ss("ZREVRANGE"), bulk(b"myzset"), ss("abc"), bulk(b"1")];
    let result = ZRevRangeCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_invalid_stop() {
    let items = vec![ss("ZREVRANGE"), bulk(b"myzset"), ss("0"), bulk(b"abc")];
    let result = ZRevRangeCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_invalid_key_type() {
    let items = vec![ss("ZREVRANGE"), Value::Integer(42), ss("0"), bulk(b"-1")];
    let result = ZRevRangeCommand::parse_args(&items);
    assert!(result.is_err());
  }

  #[test]
  fn test_format_score_integer() {
    assert_eq!(format_score(3.0), "3");
    assert_eq!(format_score(0.0), "0");
    assert_eq!(format_score(-5.0), "-5");
    assert_eq!(format_score(100.0), "100");
  }

  #[test]
  fn test_format_score_float() {
    assert_eq!(format_score(3.5), "3.5");
    assert_eq!(format_score(-2.25), "-2.25");
  }

  #[test]
  fn test_format_score_special() {
    assert_eq!(format_score(f64::INFINITY), "inf");
    assert_eq!(format_score(f64::NEG_INFINITY), "-inf");
    assert_eq!(format_score(f64::NAN), "nan");
  }

  #[test]
  fn test_build_member_prefix_hex() {
    let key = b"myzset";
    let version = 12345u64;

    let prefix_hex = build_member_prefix_hex(key, version);
    let prefix_bytes = hex::decode(&prefix_hex).unwrap();

    let key_len = u32::from_be_bytes([
      prefix_bytes[0],
      prefix_bytes[1],
      prefix_bytes[2],
      prefix_bytes[3],
    ]) as usize;
    assert_eq!(key_len, key.len());
    assert_eq!(&prefix_bytes[4..4 + key_len], key);
    assert_eq!(
      &prefix_bytes[4 + key_len..4 + key_len + 8],
      &version.to_be_bytes()
    );
  }
}
