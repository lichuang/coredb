use std::fmt;
use std::fmt::Formatter;

/// The state of an applied raft log.
/// Normally it includes two fields: the state before applying and the state after applying the log.
#[derive(
  serde::Serialize,
  serde::Deserialize,
  Debug,
  Clone,
  PartialEq,
  Eq,
  derive_more::From,
  derive_more::TryInto,
)]
pub enum AppliedState {
  #[try_into(ignore)]
  None,
}

impl fmt::Display for AppliedState {
  fn fmt(&self, f: &mut Formatter) -> fmt::Result {
    write!(f, "AppliedState: ")?;
    match self {
      AppliedState::None => {
        write!(f, "None")
      }
    }
  }
}

impl AppliedState {}
