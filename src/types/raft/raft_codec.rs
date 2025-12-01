use std::error::Error;
use std::io;

use openraft::Membership;
use openraft::StorageError;
use openraft::StoredMembership;
use openraft::alias::LogIdOf;
use openraft::alias::VoteOf;
use prost::Message;

use crate::types::protobuf as pb;
use crate::types::raft::TypeConfig;

pub(crate) trait RaftCodec {
  fn decode_from(buf: &[u8]) -> Result<Self, io::Error>
  where Self: Sized;
  fn encode_to(&self) -> Result<Vec<u8>, io::Error>;
}

impl RaftCodec for LogIdOf<TypeConfig> {
  fn decode_from(buf: &[u8]) -> Result<Self, io::Error>
  where Self: Sized {
    let log_id = crate::types::protobuf::LogId::decode(buf).map_err(read_logs_err)?;

    Ok(Self {
      leader_id: log_id.term,
      index: log_id.index,
    })
  }

  fn encode_to(&self) -> Result<Vec<u8>, io::Error> {
    let log_id = crate::types::protobuf::LogId {
      term: self.leader_id,
      index: self.index,
    };

    Ok(log_id.encode_to_vec())
  }
}

impl RaftCodec for VoteOf<TypeConfig> {
  fn decode_from(buf: &[u8]) -> Result<Self, io::Error>
  where Self: Sized {
    Ok(crate::types::protobuf::Vote::decode(buf).map_err(read_logs_err)?)
  }

  fn encode_to(&self) -> Result<Vec<u8>, io::Error> {
    Ok(self.encode_to_vec())
  }
}

impl RaftCodec for StoredMembership<TypeConfig> {
  fn decode_from(buf: &[u8]) -> Result<Self, io::Error>
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

  fn encode_to(&self) -> Result<Vec<u8>, io::Error> {
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

pub fn read_logs_err(e: impl Error + 'static) -> io::Error {
  // StorageError::read_logs(&e)
  io::Error::other(e.to_string())
}
