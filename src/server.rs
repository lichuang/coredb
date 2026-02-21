use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info, warn};

use crate::command::Command;
use crate::resp::{Parser, Value};
use crate::store::Store;

/// Default listening port (Redis default port)
const DEFAULT_PORT: u16 = 6379;

/// TCP server
pub struct Server {
    listener: TcpListener,
    local_addr: SocketAddr,
    store: Arc<Store>,
}

impl Server {
    /// Create and bind TCP server to specified address
    pub async fn bind(addr: &str) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        info!("TCP server bound to {}", local_addr);
        Ok(Self {
            listener,
            local_addr,
            store: Arc::new(Store::new()),
        })
    }

    /// Create server using default port
    pub async fn bind_default() -> std::io::Result<Self> {
        Self::bind(&format!("0.0.0.0:{}", DEFAULT_PORT)).await
    }

    /// Get local listening address
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Start server, accept and process connections
    pub async fn run(self) {
        info!("Server started, listening on {}", self.local_addr);

        loop {
            match self.listener.accept().await {
                Ok((stream, peer_addr)) => {
                    info!("New connection accepted from {}", peer_addr);
                    let store = Arc::clone(&self.store);

                    // Spawn an independent task for each connection
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, peer_addr, store).await {
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
}

/// Handle a single client connection
async fn handle_connection(
    mut stream: TcpStream,
    peer_addr: SocketAddr,
    store: Arc<Store>,
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
                            let response = process_command(value, &store);
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

/// Process a RESP command and return the response
fn process_command(value: Value, store: &Store) -> Value {
    match Command::from_resp(value) {
        Some(cmd) => cmd.execute(store),
        None => Value::error("ERR failed to parse command"),
    }
}
