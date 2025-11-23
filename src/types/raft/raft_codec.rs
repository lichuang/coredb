use std::error::Error;

use openraft::Membership;
use openraft::StorageError;
use openraft::StoredMembership;
use openraft::alias::LogIdOf;
use openraft::alias::VoteOf;
use prost::Message;

use crate::raft::protobuf as pb;
use crate::types::raft::TypeConfig;

pub(crate) trait RaftCodec {
  fn decode_from(buf: &[u8]) -> Result<Self, StorageError<TypeConfig>>
  where Self: Sized;
  fn encode_to(&self) -> Result<Vec<u8>, StorageError<TypeConfig>>;
}

impl RaftCodec for LogIdOf<TypeConfig> {
  fn decode_from(buf: &[u8]) -> Result<Self, StorageError<TypeConfig>>
  where Self: Sized {
    let log_id = crate::raft::protobuf::LogId::decode(buf).map_err(read_logs_err)?;

    Ok(Self {
      leader_id: log_id.term,
      index: log_id.index,
    })
  }

  fn encode_to(&self) -> Result<Vec<u8>, StorageError<TypeConfig>> {
    let log_id = crate::raft::protobuf::LogId {
      term: self.leader_id,
      index: self.index,
    };

    Ok(log_id.encode_to_vec())
  }
}

impl RaftCodec for VoteOf<TypeConfig> {
  fn decode_from(buf: &[u8]) -> Result<Self, StorageError<TypeConfig>>
  where Self: Sized {
    Ok(crate::raft::protobuf::Vote::decode(buf).map_err(read_logs_err)?)
  }

  fn encode_to(&self) -> Result<Vec<u8>, StorageError<TypeConfig>> {
    Ok(self.encode_to_vec())
  }
}

impl RaftCodec for StoredMembership<TypeConfig> {
  fn decode_from(buf: &[u8]) -> Result<Self, StorageError<TypeConfig>>
  where Self: Sized {
    let store_membership = pb::StoredMembership::decode(buf).map_err(read_logs_err)?;

    Ok(StoredMembership::new(
      store_membership.log_id.map(|log_id| log_id.into()),
      if let Some(membership) = store_membership.membership {
        membership.into()
      } else {
        Membership::default()
      },
    ))
  }

  fn encode_to(&self) -> Result<Vec<u8>, StorageError<TypeConfig>> {
    let store_membership = pb::StoredMembership {
      log_id: if let Some(log_id) = self.log_id() {
        Some(log_id.to_owned().into())
      } else {
        None
      },
      membership: Some(self.membership().to_owned().into()),
    };

    Ok(store_membership.encode_to_vec())
  }
}

pub fn read_logs_err(e: impl Error + 'static) -> StorageError<TypeConfig> {
  StorageError::read_logs(&e)
}
