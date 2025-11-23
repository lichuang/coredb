use std::fmt;
use std::time::Duration;

use display_more::DisplayOptionExt;
use serde::Deserialize;
use serde::Serialize;

use super::kv_meta::KVMeta;
use crate::types::operation::Operation;
use crate::types::with::With;

/// Update or insert a general purpose kv store
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, deepsize::DeepSizeOf)]
pub struct UpsertKV {
  pub key: String,

  /// The value to set. A `None` indicates to delete it.
  pub value: Operation<Vec<u8>>,

  /// Meta data of a value.
  pub meta: Option<KVMeta>,
}

impl fmt::Display for UpsertKV {
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

impl UpsertKV {
  pub fn new(key: impl ToString, value: Operation<Vec<u8>>, meta: Option<KVMeta>) -> Self {
    Self {
      key: key.to_string(),
      value,
      meta,
    }
  }

  pub fn insert(key: impl ToString, value: &[u8]) -> Self {
    Self {
      key: key.to_string(),
      value: Operation::Update(value.to_vec()),
      meta: None,
    }
  }

  pub fn update(key: impl ToString, value: &[u8]) -> Self {
    Self {
      key: key.to_string(),
      value: Operation::Update(value.to_vec()),
      meta: None,
    }
  }

  pub fn delete(key: impl ToString) -> Self {
    Self {
      key: key.to_string(),
      value: Operation::Delete,
      meta: None,
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

impl With<KVMeta> for UpsertKV {
  fn with(mut self, meta: KVMeta) -> Self {
    self.meta = Some(meta);
    self
  }
}
