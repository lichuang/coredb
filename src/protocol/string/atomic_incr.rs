//! Shared atomic increment helper for INCR/INCRBY/DECR/DECRBY.
//!
//! Atomicity comes from the library transaction: the compare condition and the
//! write are evaluated together in a single apply, so concurrent increments
//! cannot lose updates. Encoding and integer semantics stay in this application
//! layer — the state machine only ever sees opaque bytes and must never be
//! asked to parse or arithmetic on values.

use rockraft::raft::types::{TxnCondition, TxnReply, TxnReq, UpsertKV};

use crate::encoding::{NO_EXPIRATION, StringValue};
use crate::error::{CoreDbError, ProtocolError};
use crate::server::Server;
use crate::util::{MAX_CAS_RETRIES, now_ms};

fn parse_i64(data: &[u8]) -> Option<i64> {
  std::str::from_utf8(data).ok()?.trim().parse::<i64>().ok()
}

/// Atomically add `delta` to the integer stored at `key`, returning the new value.
///
/// Treats a missing or expired key as 0, and rejects non-integer or
/// wrong-type values with the matching protocol error before writing anything.
pub async fn atomic_incr(server: &Server, key: &str, delta: i64) -> Result<i64, CoreDbError> {
  let now = now_ms();

  for _ in 0..MAX_CAS_RETRIES {
    let raw = server.get(key).await?;

    let (current_int, expires_at, condition) = match raw {
      Some(bytes) => match StringValue::deserialize(&bytes) {
        Ok(sv) => {
          if sv.is_expired(now) {
            // Expired values count as absent. CAS against the stale bytes so
            // the increment only applies if nothing else touched the key;
            // the stale TTL is dropped since the value is being resurrected.
            (0, NO_EXPIRATION, TxnCondition::eq(key, &bytes))
          } else {
            let n = parse_i64(&sv.data).ok_or(ProtocolError::NotAnInteger)?;
            (n, sv.expires_at, TxnCondition::eq(key, &bytes))
          }
        }
        Err(_) => return Err(ProtocolError::WrongType.into()),
      },
      None => (0, NO_EXPIRATION, TxnCondition::not_exists(key)),
    };

    let new_int = current_int
      .checked_add(delta)
      .ok_or(ProtocolError::Overflow)?;

    let new_value = if expires_at == NO_EXPIRATION {
      StringValue::new(new_int.to_string().into_bytes())
    } else {
      StringValue::with_expiration(new_int.to_string().into_bytes(), expires_at)
    };

    let txn = TxnReq::new(vec![condition]).if_then(UpsertKV::insert(key, &new_value.serialize()));

    match server.txn(txn).await? {
      TxnReply::Success { branch: true, .. } => return Ok(new_int),
      // Lost the race against a concurrent writer; re-read and retry.
      TxnReply::Success { branch: false, .. } => continue,
    }
  }

  Err(ProtocolError::Custom("ERR increment retry limit exceeded").into())
}

#[cfg(test)]
mod tests {
  use super::*;

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
