use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info, warn};

use crate::protocol::{CommandFactory, Parser, Value};
use crate::store::Store;

/// Default listening port (Redis default port)
const DEFAULT_PORT: u16 = 6379;

/// TCP server
pub struct Server {
    listener: TcpListener,
    local_addr: SocketAddr,
    cmd_factory: Arc<CommandFactory>,
    store: Arc<Store>,
}

impl Server {
    /// Create and bind TCP server to specified address
    pub async fn bind(addr: &str) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        info!("TCP server bound to {}", local_addr);

        // Initialize command factory
        let cmd_factory = Arc::new(CommandFactory::init());
        let store = Arc::new(Store::new());

        Ok(Self {
            listener,
            local_addr,
            cmd_factory,
            store,
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

    /// Get a value from the store
    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        self.store.get(key)
    }

    /// Set a value in the store
    pub async fn set(&self, key: String, value: Vec<u8>) -> Result<(), String> {
        self.store.set(key, value)
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
}
