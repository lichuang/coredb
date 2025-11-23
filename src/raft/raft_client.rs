use tonic::transport::channel::Channel;
use tracing::debug;

use super::endpoint::Endpoint;
use crate::config::GrpcConfig;

/// Defines the API of the client to a raft node.
pub trait RaftClientApi {
  fn new(target: NodeId, endpoint: Endpoint, channel: Channel) -> Self;
  // fn endpoint(&self) -> &Endpoint;
}

impl RaftClientApi for RaftServiceClient<Channel> {
  fn new(target: NodeId, endpoint: Endpoint, channel: Channel) -> Self {
    let endpoint_str = endpoint.to_string();

    debug!(
      "RaftClient::new: target: {} endpoint: {}",
      target, endpoint_str
    );

    RaftServiceClient::new(channel)
      .max_decoding_message_size(GrpcConfig::MAX_DECODING_SIZE)
      .max_encoding_message_size(GrpcConfig::MAX_ENCODING_SIZE)
  }

  // fn endpoint(&self) -> &Endpoint {
  // &self.endpoint
  // }
}
