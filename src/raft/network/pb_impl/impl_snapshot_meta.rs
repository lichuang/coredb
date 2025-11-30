use crate::types::protobuf as pb;
use crate::types::raft::SnapshotMeta;

impl From<SnapshotMeta> for pb::SnapshotMeta {
  fn from(meta: SnapshotMeta) -> Self {
    pb::SnapshotMeta {
      last_log_id: meta.last_log_id.map(|log_id| log_id.into()),
      last_membership: Some(meta.last_membership.into()),
      snapshot_id: meta.snapshot_id,
    }
  }
}

impl From<pb::SnapshotMeta> for SnapshotMeta {
  fn from(value: pb::SnapshotMeta) -> Self {
    Self {
      last_log_id: value.last_log_id.map(|log_id| log_id.into()),
      last_membership: value.last_membership.unwrap().into(),
      snapshot_id: value.snapshot_id,
    }
  }
}
