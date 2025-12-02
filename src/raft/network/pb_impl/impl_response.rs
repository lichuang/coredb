use std::fmt;

use crate::types::protobuf as pb;

impl fmt::Display for pb::Response {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "resp {}", self.value())
  }
}
