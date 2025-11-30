use crate::types::protobuf as pb;
use crate::types::raft::RaftSnapshotData;

impl From<pb::SnapshotData> for RaftSnapshotData {
  fn from(value: pb::SnapshotData) -> Self {
    value
      .data
      .into_iter()
      .map(|keyvalue| (keyvalue.key, keyvalue.value))
      .collect()
  }
}

impl From<RaftSnapshotData> for pb::SnapshotData {
  fn from(value: RaftSnapshotData) -> Self {
    let data = value
      .into_iter()
      .map(|keyvalue| pb::KeyValue {
        key: keyvalue.0,
        value: keyvalue.1,
      })
      .collect();
    Self { data }
  }
}
