use std::fmt;

use crate::types::protobuf as pb;

impl fmt::Display for pb::SetRequest {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "set {} to value len {}", self.key, self.value.len())
  }
}
