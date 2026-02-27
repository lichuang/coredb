use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info, warn};

use crate::config::Config;
use crate::protocol::{CommandFactory, Parser, Value};

/// TCP server with Raft support
pub struct Server {
  listener: TcpListener,
  local_addr: SocketAddr,
  cmd_factory: Arc<CommandFactory>,
  /// Raft node for distributed consensus
  raft_node: Arc<rockraft::node::RaftNode>,
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
  pub async fn start(config: Config) -> Result<Arc<Self>, Box<dyn std::error::Error>> {
    // Create and start Raft node
    info!("Creating Raft node...");
    let raft_node = rockraft::node::RaftNodeBuilder::build(&config.raft)
      .await
      .map_err(|e| format!("Failed to create Raft node: {}", e))?;
    info!("Raft node created and started successfully");

    // Bind TCP server
    let listener = TcpListener::bind(&config.server_addr).await?;
    let local_addr = listener.local_addr()?;
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
  pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
    // Use RaftNode's read for consistent read
    let req = rockraft::raft::types::GetKVReq {
      key: key.to_string(),
    };
    match self.raft_node.read(req).await {
      Ok(value) => Ok(value),
      Err(e) => Err(format!("Failed to read: {}", e)),
    }
  }

  /// Set a value in the store (through Raft consensus)
  pub async fn set(&self, key: String, value: Vec<u8>) -> Result<(), String> {
    // Create UpsertKV command
    let upsert_kv =
      rockraft::raft::types::Cmd::UpsertKV(rockraft::raft::types::UpsertKV::insert(&key, &value));
    let entry = rockraft::raft::types::LogEntry::new(upsert_kv);

    // Write through Raft (will be forwarded to leader if needed)
    match self.raft_node.write(entry).await {
      Ok(_) => Ok(()),
      Err(e) => Err(format!("Failed to write: {}", e)),
    }
  }

  /// Delete a key from the store (through Raft consensus)
  pub async fn delete(&self, key: &str) -> Result<bool, String> {
    // Create Delete command
    let upsert_kv =
      rockraft::raft::types::Cmd::UpsertKV(rockraft::raft::types::UpsertKV::delete(key));
    let entry = rockraft::raft::types::LogEntry::new(upsert_kv);

    // Write through Raft (will be forwarded to leader if needed)
    match self.raft_node.write(entry).await {
      Ok(_) => Ok(true),
      Err(e) => Err(format!("Failed to delete: {}", e)),
    }
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
          loop {
            match Parser::parse(&pending[processed..]) {
              Some((value, consumed)) => {
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
              None => {
                // No complete command available
                break;
              }
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
  pub async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error>> {
    info!("Shutting down Raft node...");
    self.raft_node.shutdown().await?;
    info!("Raft node shutdown successfully");
    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  // Note: These tests would require a running Raft node
  // For now, we just verify the Server structure compiles correctly
  #[test]
  fn test_server_structure() {
    // This test ensures the Server struct compiles with RaftNode
    // Actual tests would need a mock RaftNode or integration setup
  }
}
