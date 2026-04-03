use std::net::SocketAddr;
use std::sync::Arc;

use rockraft::node::{RaftNode, RaftNodeBuilder};
use rockraft::raft::types::{
  AppliedState, BatchWriteReq, Cmd, GetKVReq, LogEntry, ScanPrefixReq, UpsertKV,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info, warn};

use crate::config::Config;
use crate::error::{CoreDbError, ServerError, StorageError};
use crate::protocol::{CommandFactory, Parser, Value};

/// TCP server with Raft support
pub struct Server {
  listener: TcpListener,
  local_addr: SocketAddr,
  cmd_factory: Arc<CommandFactory>,
  /// Raft node for distributed consensus
  raft_node: Arc<RaftNode>,
  /// Server configuration
  config: Config,
}

impl Server {
  /// Create and start the server with Raft node
  ///
  /// This function:
  /// 1. Creates and starts the Raft node
  /// 2. Binds the TCP server
  /// 3. Returns the initialized Server instance
  pub async fn start(config: Config) -> Result<Arc<Self>, CoreDbError> {
    // Create and start Raft node
    info!("Creating Raft node...");
    let raft_node = RaftNodeBuilder::build(&config.raft)
      .await
      .map_err(|e| StorageError::Raft(format!("Failed to create Raft node: {}", e)))?;
    info!("Raft node created and started successfully");

    // Bind TCP server
    let listener =
      TcpListener::bind(&config.server_addr)
        .await
        .map_err(|e| ServerError::BindFailed {
          addr: config.server_addr.to_string(),
          reason: e.to_string(),
        })?;
    let local_addr = listener.local_addr().map_err(|e| ServerError::BindFailed {
      addr: config.server_addr.to_string(),
      reason: e.to_string(),
    })?;
    info!("TCP server bound to {}", local_addr);

    // Initialize command factory
    let cmd_factory = Arc::new(CommandFactory::init());

    let server = Arc::new(Self {
      listener,
      local_addr,
      cmd_factory,
      raft_node,
      config,
    });

    Ok(server)
  }

  /// Get local listening address
  pub fn local_addr(&self) -> SocketAddr {
    self.local_addr
  }

  /// Get a value from the store (local read)
  pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
    let req = GetKVReq {
      key: key.to_string(),
    };
    self
      .raft_node
      .read(req)
      .await
      .map_err(|e| StorageError::ReadFailed(e.to_string()))
  }

  /// Set a value in the store (through Raft consensus)
  pub async fn set(&self, key: String, value: Vec<u8>) -> Result<(), StorageError> {
    let upsert_kv = Cmd::UpsertKV(UpsertKV::insert(&key, &value));
    let entry = LogEntry::new(upsert_kv);

    self
      .raft_node
      .write(entry)
      .await
      .map_err(|e| StorageError::WriteFailed(e.to_string()))?;

    Ok(())
  }

  /// Delete a key from the store (through Raft consensus)
  pub async fn delete(&self, key: &str) -> Result<bool, StorageError> {
    let upsert_kv = Cmd::UpsertKV(UpsertKV::delete(key));
    let entry = LogEntry::new(upsert_kv);

    self
      .raft_node
      .write(entry)
      .await
      .map_err(|e| StorageError::DeleteFailed(e.to_string()))?;

    Ok(true)
  }

  /// Batch write multiple entries atomically (through Raft consensus)
  ///
  /// This ensures all entries are written as a single atomic operation.
  /// Either all entries are applied, or none are.
  pub async fn batch_write(&self, entries: Vec<UpsertKV>) -> Result<AppliedState, StorageError> {
    let req = BatchWriteReq { entries };
    self
      .raft_node
      .batch_write(req)
      .await
      .map_err(|e| StorageError::WriteFailed(e.to_string()))
  }

  /// Scan keys by prefix from the state machine (forwarded to leader)
  /// Returns a vector of (key, value) tuples where keys start with the given prefix
  pub async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
    let req = ScanPrefixReq {
      prefix: prefix.to_vec(),
    };
    self
      .raft_node
      .scan_prefix(req)
      .await
      .map_err(|e| StorageError::ReadFailed(e.to_string()))
  }

  /// Process a RESP command and return the response
  async fn process_command(&self, value: Value) -> Value {
    self.cmd_factory.execute(value, self).await
  }

  /// Handle a single client connection
  async fn handle_connection(
    self: Arc<Self>,
    mut stream: TcpStream,
    peer_addr: SocketAddr,
  ) -> std::io::Result<()> {
    let mut buffer = vec![0u8; 8192]; // 8KB buffer
    let mut pending = Vec::new(); // Buffer for incomplete commands

    loop {
      match stream.read(&mut buffer).await {
        Ok(0) => {
          info!("Connection closed by client: {}", peer_addr);
          break;
        }
        Ok(n) => {
          // Append new data to pending buffer
          pending.extend_from_slice(&buffer[..n]);

          // Try to parse and process complete commands
          let mut processed = 0;
          while let Some((value, consumed)) = Parser::parse(&pending[processed..]) {
            processed += consumed;

            // Log the parsed command
            info!("Received command from {}: {:?}", peer_addr, value);

            // Process the command and get response
            let response = self.process_command(value).await;
            let encoded = response.encode();

            // Send response
            if let Err(e) = stream.write_all(&encoded).await {
              warn!("Failed to write response to {}: {}", peer_addr, e);
              break;
            }
          }

          // Remove processed data from pending buffer
          if processed > 0 {
            pending = pending.split_off(processed);
          }
        }
        Err(e) => {
          error!("Error reading from {}: {}", peer_addr, e);
          break;
        }
      }
    }

    info!("Connection handler ended for {}", peer_addr);
    Ok(())
  }

  /// Start server, accept and process connections
  pub async fn run(self: Arc<Self>) {
    info!("Server started, listening on {}", self.local_addr);
    info!("Raft node ID: {}", self.config.raft.node_id);
    info!("Raft address: {}", self.config.raft.raft.address);

    loop {
      match self.listener.accept().await {
        Ok((stream, peer_addr)) => {
          info!("New connection accepted from {}", peer_addr);

          // Clone the Arc<Server> for the new connection
          let server = Arc::clone(&self);

          // Spawn an independent task for each connection
          tokio::spawn(async move {
            if let Err(e) = server.handle_connection(stream, peer_addr).await {
              error!("Error handling connection from {}: {}", peer_addr, e);
            }
          });
        }
        Err(e) => {
          error!("Failed to accept connection: {}", e);
        }
      }
    }
  }

  /// Shutdown the server and Raft node
  pub async fn shutdown(&self) -> Result<(), CoreDbError> {
    info!("Shutting down Raft node...");
    self
      .raft_node
      .shutdown()
      .await
      .map_err(|e| ServerError::Connection(format!("Shutdown failed: {}", e)))?;
    info!("Raft node shutdown successfully");
    Ok(())
  }
}

#[cfg(test)]
mod tests {
  // Note: These tests would require a running Raft node
  // For now, we just verify the Server structure compiles correctly
  #[test]
  fn test_server_structure() {
    // This test ensures the Server struct compiles with RaftNode
    // Actual tests would need a mock RaftNode or integration setup
  }
}
