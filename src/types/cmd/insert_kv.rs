use std::fmt;
use std::time::Duration;

use display_more::DisplayOptionExt;
use serde::Deserialize;
use serde::Serialize;

use super::kv_meta::KVMeta;
use crate::types::with::With;

/// Update or insert a general purpose kv store
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, deepsize::DeepSizeOf)]
pub struct InsertKV {
  pub key: String,

  pub value: Vec<u8>,

  /// Meta data of a value.
  pub meta: Option<KVMeta>,
}

impl fmt::Display for InsertKV {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(
      f,
      "{} = {:?} ({})",
      self.key,
      self.value,
      self.meta.display()
    )
  }
}

impl InsertKV {
  pub fn new(key: impl ToString, value: Vec<u8>, meta: Option<KVMeta>) -> Self {
    Self {
      key: key.to_string(),
      value,
      meta,
    }
  }

  pub fn with_expire_sec(self, expire_at_sec: u64) -> Self {
    self.with(KVMeta::new_expire(expire_at_sec))
  }

  /// Set the time to last for the value.
  /// When the ttl is passed, the value is deleted.
  pub fn with_ttl(self, ttl: Duration) -> Self {
    self.with(KVMeta::new_ttl(ttl))
  }
}

impl With<KVMeta> for InsertKV {
  fn with(mut self, meta: KVMeta) -> Self {
    self.meta = Some(meta);
    self
  }
}
