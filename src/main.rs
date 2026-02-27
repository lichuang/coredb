mod config;
mod encoding;
mod protocol;
mod server;
mod util;

use std::env;
use std::sync::Arc;
use tokio::signal;
use tracing::{error, info};

use config::Config;
use server::Server;

#[tokio::main]
async fn main() -> std::io::Result<()> {
  // Initialize logging
  tracing_subscriber::fmt()
    .with_env_filter(
      tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
    )
    .with_target(true)
    .with_thread_ids(true)
    .init();

  info!("Starting CoreDB - Redis compatible distributed KV store");
  info!("Version: 0.1.0");

  // Parse command line arguments to get config path
  let args: Vec<String> = env::args().collect();
  let config_path = if args.len() > 2 && args[1] == "--conf" {
    args[2].clone()
  } else {
    eprintln!("Usage: {} --conf <config-file>", args[0]);
    eprintln!("Example: {} --conf conf/node1.toml", args[0]);
    std::process::exit(1);
  };

  // Load configuration
  let config = match Config::from_file(&config_path) {
    Ok(cfg) => cfg,
    Err(e) => {
      error!("Failed to load configuration: {}", e);
      std::process::exit(1);
    }
  };

  info!("Configuration loaded:");
  info!("  node_id: {}", config.raft.node_id);
  info!("  server_addr: {}", config.server_addr);
  info!("  raft_addr: {}", config.raft.raft.address);
  info!("  data_path: {}", config.raft.rocksdb.data_path);
  info!("  single: {}", config.raft.raft.single);
  info!("  join: {:?}", config.raft.raft.join);

  // Create and start server (which creates Raft node internally)
  let server = match Server::start(config).await {
    Ok(srv) => {
      info!("Server started successfully");
      info!("Listening on: {}", srv.local_addr());
      srv
    }
    Err(e) => {
      error!("Failed to start server: {}", e);
      std::process::exit(1);
    }
  };

  // Clone server for signal handling
  let server_for_shutdown = Arc::clone(&server);

  // Spawn server in a separate task
  let server_handle = tokio::spawn(async move {
    server.run().await;
  });

  // Wait for Ctrl+C signal
  info!("Press Ctrl+C to shutdown...");
  match signal::ctrl_c().await {
    Ok(()) => {
      info!("Received shutdown signal");
    }
    Err(e) => {
      error!("Failed to listen for ctrl_c signal: {}", e);
    }
  }

  // Shutdown Raft node
  if let Err(e) = server_for_shutdown.shutdown().await {
    error!("Error during shutdown: {}", e);
  }

  // Abort server task
  server_handle.abort();

  info!("Server shutdown complete");
  Ok(())
}
