mod endpoint;
mod network;
mod store;
// mod raft_client;

#[allow(clippy::all)]
pub mod protobuf {
  tonic::include_proto!("openraftpb");
}

use std::sync::Arc;

use network::NetworkFactory;
pub use network::RaftServiceImpl;

use crate::config::Config;
use crate::errors::Result;
pub(crate) use crate::raft::store::new_storage;
pub use crate::types::raft::raft_types::*;
// pub use storage::new_raft_storage;

pub async fn new_raft(config: &Config) -> Result<Raft> {
  let raft_config = Arc::new(openraft::Config::default());
  let network = NetworkFactory::new();

  let node_id = config.node_id;
  let dir = &config.data_dir;

  let (log_store, state_machine) = new_storage(dir).await?;

  let ret = Raft::new(node_id, raft_config, network, log_store, state_machine).await;
  match ret {
    Ok(raft) => Ok(raft),
    Err(e) => {
      let open_raft_err: OpenRaftError = e.into();
      Err(open_raft_err.into())
    }
  }
}
