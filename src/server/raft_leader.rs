use std::time::Duration;
use std::time::SystemTime;

use tracing::info;

use super::server::Server;
use crate::raft::ClientWriteError;
use crate::raft::RaftError;
use crate::types::applied_state::AppliedState;
use crate::types::log_entry::LogEntry;

/// The container of APIs of the leader in a coredb service cluster.
///
/// A leader does not imply it is actually the leader granted by the cluster.
/// It just means it believes it is the leader and have not yet perceived there is other newer leader.
pub struct RaftLeader<'a> {
  server: &'a Server,
}

impl<'a> RaftLeader<'a> {
  pub fn new(server: &'a Server) -> RaftLeader<'a> {
    RaftLeader { server }
  }

  /// Write a log through local raft node and return the states before and after applying the log.
  ///
  /// If the raft node is not a leader, it returns MetaRaftError::ForwardToLeader.
  pub async fn write(
    &self,
    mut entry: LogEntry,
  ) -> Result<AppliedState, RaftError<ClientWriteError>> {
    // Add consistent clock time to log entry.
    entry.time_ms = Some(since_epoch().as_millis() as u64);

    // report metrics
    // let _guard = ProposalPending::guard();

    info!("write LogEntry: {}", entry);
    let write_res = self.server.raft.client_write(entry).await;

    match write_res {
      Ok(resp) => {
        info!(
          "raft.client_write res ok: log_id: {}, data: {}, membership: {:?}",
          resp.log_id, resp.data, resp.membership
        );
        Ok(resp.data)
      }
      Err(raft_err) => {
        // server_metrics::incr_proposals_failed();
        info!("raft.client_write res err: {:?}", raft_err);
        Err(raft_err)
      }
    }
  }
}

fn since_epoch() -> Duration {
  SystemTime::now()
    .duration_since(SystemTime::UNIX_EPOCH)
    .unwrap()
}
