use std::fmt;
use std::fmt::Formatter;
use std::time::Duration;

use deepsize::Context;
use display_more::DisplayUnixTimeStampExt;

use crate::types::time::Interval;
use crate::types::time::flexible_timestamp_to_duration;

/// Specifies the metadata associated with a kv record, used in an `upsert` cmd.
///
/// This is similar to [`KVMeta`] but differs, [`KVMeta`] is used in storage,
/// as this instance is employed for transport purposes.
/// When an `upsert` cmd is applied, this instance is evaluated and a `KVMeta` is built.
#[derive(serde::Serialize, serde::Deserialize, Debug, Default, Clone, Eq, PartialEq)]
pub struct KVMeta {
  /// Expiration time in **seconds or milliseconds** since Unix epoch (1970-01-01).
  ///
  /// The interpretation depends on the magnitude of the value:
  /// - Values > `100_000_000_000`: treated as milliseconds since epoch
  /// - Values ≤ `100_000_000_000`: treated as seconds since epoch
  ///
  /// See [`flexible_timestamp_to_duration`]
  pub(crate) expire_at: Option<u64>,

  /// Relative expiration time interval since when the raft log is applied.
  ///
  /// Use this field if possible to avoid the clock skew between client and meta-service.
  /// `expire_at` may already be expired when it is applied to state machine.
  ///
  /// If it is not None, once applied, the `expire_at` field will be replaced with the calculated absolute expiration time.
  ///
  /// For backward compatibility, this field is not serialized if it `None`, as if it does not exist.
  #[serde(skip_serializing_if = "Option::is_none")]
  pub(crate) ttl: Option<Interval>,
}

impl deepsize::DeepSizeOf for KVMeta {
  fn deep_size_of_children(&self, _context: &mut Context) -> usize {
    0
  }
}

impl fmt::Display for KVMeta {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(f, "KVMeta(",)?;

    if let Some(expires_at) = self.expire_at {
      write!(
        f,
        "expire_at: {} ",
        flexible_timestamp_to_duration(expires_at).display_unix_timestamp_short()
      )?;
    }
    if let Some(ttl) = &self.ttl {
      write!(f, "ttl: {:?} ", Duration::from_millis(ttl.millis()))?;
    }

    write!(f, ")")?;
    Ok(())
  }
}

impl KVMeta {
  /// Create a new KVMeta
  ///
  /// `expires_at_sec_or_ms`: absolute expiration time in **seconds or milliseconds** since 1970-01-01.
  pub fn new(expires_at_sec_or_ms: Option<u64>, ttl: Option<Interval>) -> Self {
    Self {
      expire_at: expires_at_sec_or_ms,
      ttl,
    }
  }

  /// Create a KVMeta with an absolute expiration time in second since 1970-01-01.
  pub fn new_expire(expires_at_sec_or_ms: u64) -> Self {
    Self {
      expire_at: Some(expires_at_sec_or_ms),
      ttl: None,
    }
  }

  /// Create a KVMeta with relative expiration time(ttl).
  pub fn new_ttl(ttl: Duration) -> Self {
    Self {
      expire_at: None,
      ttl: Some(Interval::from_duration(ttl)),
    }
  }
}

#[cfg(test)]
mod tests {
  use std::time::Duration;

  use super::KVMeta;
  use crate::Time;
  use crate::cmd::CmdContext;

  #[test]
  fn test_serde() {
    let meta = KVMeta::new_expire(1723102819);
    let s = serde_json::to_string(&meta).unwrap();
    assert_eq!(r#"{"expire_at":1723102819}"#, s);

    let meta = KVMeta::new_ttl(Duration::from_millis(100));
    let s = serde_json::to_string(&meta).unwrap();
    assert_eq!(r#"{"expire_at":null,"ttl":{"millis":100}}"#, s);
  }
}
