use super::Server;
use crate::errors::RaftAPIError;
use crate::raft::NodeId;
use crate::types::protobuf::ForwardRequest;
use crate::types::protobuf::ForwardResponse;

/// Handle a request locally if it is leader. Otherwise, forward it to the leader.
pub struct RaftForwarder<'a> {
  server: &'a Server,
}

impl<'a> RaftForwarder<'a> {
  pub fn new(server: &'a Server) -> Self {
    Self { server }
  }

  pub async fn forward(
    &self,
    target: NodeId,
    req: &mut ForwardRequest,
  ) -> Result<ForwardResponse, RaftAPIError> {
    Ok(ForwardResponse { response: None })
  }
}
