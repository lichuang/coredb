use crate::errors::Result;
use crate::raft::NodeId;
use crate::raft::RocksLogStore;
use crate::raft::RocksStateMachine;
use crate::raft::TypeConfig;
use crate::types::protobuf::Node;

#[derive(Clone)]
pub struct RaftStore {
  pub log_store: RocksLogStore<TypeConfig>,

  pub state_machine: RocksStateMachine,
}

impl RaftStore {
  pub fn new(log_store: RocksLogStore<TypeConfig>, state_machine: RocksStateMachine) -> Self {
    Self {
      log_store,
      state_machine,
    }
  }

  pub fn get_node(&self, node_id: &NodeId) -> Result<Option<Node>> {
    let (_, last_membership) = self.state_machine.get_meta()?;
    let membership = last_membership.membership();
    Ok(membership.get_node(node_id).cloned())
  }
}
