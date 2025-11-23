use crate::raft::protobuf as pb;
use crate::types::raft::Snapshot;

impl From<Snapshot> for pb::SnapshotFile {
  fn from(snapshot: Snapshot) -> Self {
    pb::SnapshotFile {
      meta: Some(snapshot.meta.into()),
      data: Some(snapshot.snapshot.into()),
    }
  }
}

impl From<pb::SnapshotFile> for Snapshot {
  fn from(value: pb::SnapshotFile) -> Self {
    Self {
      meta: value.meta.unwrap().into(),
      snapshot: value.data.unwrap().into(),
    }
  }
}
