mod connection;
mod kv_api;
mod server;
mod shutdown;
mod store;

use std::sync::Arc;

pub use connection::Connection;
pub use kv_api::KVApi;
use server::Server;
pub use shutdown::Shutdown;
use store::RaftStore;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::sync::broadcast;
use tokio::sync::watch;
use tracing::error;
use tracing::info;

use crate::config::Config;
use crate::errors::Result;

const DEFAULT_PORT: u16 = 6379;

pub async fn run(config: Config, shutdown: impl Future) -> Result<()> {
  println!("listen: {}", DEFAULT_PORT);
  let listener = TcpListener::bind(&format!("127.0.0.1:{}", DEFAULT_PORT)).await?;

  let (notify_shutdown, _) = broadcast::channel(1);

  let (raft, log_store, state_machine) = crate::raft::new_raft(&config).await?;

  let raft_store = RaftStore::new(log_store, state_machine);

  let (tx, rx) = watch::channel::<()>(());

  let mut server = Server {
    listener,
    config,
    notify_shutdown,
    running_tx: tx,
    running_rx: rx,
    join_handles: Mutex::new(Vec::new()),
    raft: Arc::new(raft),
    raft_store,
  };

  server.start().await?;

  tokio::select! {
      res = server.run() => {
          if let Err(err) = res {
              error!(cause = %err, "failed to accept");
          }
      }
      _ = shutdown => {
          // The shutdown signal has been received.
          info!("coredb server shutting down");
      }
  }

  server.shutdown().await;

  Ok(())
}
