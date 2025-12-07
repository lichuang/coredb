use anyerror::AnyError;

use super::NetworkError;
use crate::raft::ForwardToLeader;

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum RaftAPIError {
  /// If a request can only be dealt with by a leader, it informs the caller to forward the request to a leader.
  #[error(transparent)]
  ForwardToLeader(#[from] ForwardToLeader),

  #[error("can not forward any more: {0}")]
  CanNotForward(AnyError),

  /// Network error when sending a request to the leader.
  #[error(transparent)]
  NetworkError(#[from] NetworkError),
}
