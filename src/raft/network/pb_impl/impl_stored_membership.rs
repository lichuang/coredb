use openraft::StoredMembership;

use crate::raft::protobuf as pb;
use crate::types::raft::TypeConfig;

impl From<pb::StoredMembership> for StoredMembership<TypeConfig> {
  fn from(value: pb::StoredMembership) -> Self {
    Self::new(
      value.log_id.map(|log_id| log_id.into()),
      value.membership.unwrap().into(),
    )
  }
}

impl From<StoredMembership<TypeConfig>> for pb::StoredMembership {
  fn from(value: StoredMembership<TypeConfig>) -> Self {
    Self {
      log_id: value.log_id().map(|log_id| log_id.into()),
      membership: Some(value.membership().to_owned().into()),
    }
  }
}
