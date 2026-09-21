mod config;
mod encoding;
mod error;
mod protocol;
mod server;
mod util;

use std::env;
use std::process::exit;
use std::sync::Arc;

use tokio::signal;
use tracing::{error, info};

use config::{Config, LogConfig};
use server::Server;
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Initialize tracing from the config file: `config.log.level` drives the
/// filter unless `RUST_LOG` overrides it; `config.log.file` redirects output
/// to a file (otherwise stdout).
fn init_logging(log: &LogConfig) -> Result<(), Box<dyn std::error::Error>> {
  let env_filter = if env::var("RUST_LOG").is_ok() {
    tracing_subscriber::EnvFilter::try_from_default_env()
      .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&log.level))
  } else {
    tracing_subscriber::EnvFilter::new(&log.level)
  };

  match &log.file {
    Some(path) => {
      if let Some(parent) = std::path::Path::new(path).parent() {
        std::fs::create_dir_all(parent)?;
      }
      let file_appender = tracing_appender::rolling::never(
        std::path::Path::new(path)
          .parent()
          .unwrap_or(std::path::Path::new(".")),
        std::path::Path::new(path).file_name().unwrap_or_default(),
      );
      tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer().with_writer(file_appender))
        .init();
    }
    None => {
      tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .with_thread_ids(true)
        .init();
    }
  }

  Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  // Parse command line arguments to get config path first (logging config
  // lives in the config file, so it must be loaded before initializing logs)
  let args: Vec<String> = env::args().collect();
  let config_path = if args.len() > 2 && args[1] == "--conf" {
    args[2].clone()
  } else {
    eprintln!("Usage: {} --conf <config-file>", args[0]);
    eprintln!("Example: {} --conf conf/node1.toml", args[0]);
    exit(1);
  };

  // Load configuration
  let config = match Config::from_file(&config_path) {
    Ok(cfg) => cfg,
    Err(e) => {
      eprintln!("Failed to load configuration: {}", e);
      exit(1);
    }
  };

  // Initialize logging from config: RUST_LOG overrides, otherwise
  // config.log.level; optionally redirect to config.log.file.
  init_logging(&config.log)?;

  info!("Starting CoreDB - Redis compatible distributed KV store");
  info!("Version: 0.1.0");

  info!("Configuration loaded:");
  info!("  node_id: {}", config.raft.node_id);
  info!("  server_addr: {}", config.server_addr);
  info!("  raft_addr: {}", config.raft.raft.endpoint);
  info!("  data_path: {}", config.raft.rocksdb.data_path);
  info!("  join: {:?}", config.raft.raft.join);
  info!("  log_level: {}", config.log.level);

  // Create and start server (which creates Raft node internally)
  let server = match Server::start(config).await {
    Ok(srv) => {
      info!("Server started successfully");
      info!("Listening on: {}", srv.local_addr());
      srv
    }
    Err(e) => {
      error!("Failed to start server: {}", e);
      exit(1);
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
