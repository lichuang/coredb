use std::fmt;

pub use insert_kv::InsertKV;
pub use kv_meta::KVMeta;
use serde::Deserialize;
use serde::Serialize;

mod insert_kv;
mod kv_meta;

/// A Cmd describes what a user want to do to raft state machine
/// and is the essential part of a raft log.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, deepsize::DeepSizeOf)]
pub enum Cmd {
  InsertKV(InsertKV),
}

impl fmt::Display for Cmd {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match self {
      Cmd::InsertKV(insert_kv) => {
        write!(f, "insert_kv:{}", insert_kv)
      }
    }
  }
}
