use std::fmt;

use serde::Deserialize;
use serde::Serialize;
pub use upsert_kv::UpsertKV;

mod kv_meta;
mod upsert_kv;

/// A Cmd describes what a user want to do to raft state machine
/// and is the essential part of a raft log.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, deepsize::DeepSizeOf)]
pub enum Cmd {
  UpsertKV(UpsertKV),
}

impl fmt::Display for Cmd {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match self {
      Cmd::UpsertKV(upsert_kv) => {
        write!(f, "upsert_kv:{}", upsert_kv)
      }
    }
  }
}
