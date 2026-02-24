mod encoding;
mod protocol;
mod server;
mod store;
mod util;

use std::sync::Arc;
use server::Server;
use tracing::info;

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

    info!("Starting CoreDB - Redis compatible KV store");
    info!("Version: 0.1.0");

    // Create and start TCP server
    let server = Arc::new(Server::bind_default().await?);
    info!("Server listening on: {}", server.local_addr());

    // Start server (blocking)
    server.run().await;

    Ok(())
}
