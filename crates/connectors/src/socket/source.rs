//! Socket text source connector.
//!
//! Reads lines from TCP connections and emits them as string events.

use super::config::{SocketConfig, SocketMode};
use anyhow::Result;
use bicycle_runtime::Emitter;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

/// A socket source that reads lines from TCP connections.
///
/// # Modes
///
/// - **Server mode**: Binds to a port, accepts connections, and reads lines
///   from all connected clients.
/// - **Client mode**: Connects to a server and reads lines from it.
///
/// # Example
///
/// ```ignore
/// use bicycle_connectors::socket::{SocketTextSource, SocketConfig};
/// use bicycle_runtime::Emitter;
///
/// let config = SocketConfig::server(9999);
/// let mut source = SocketTextSource::new(config);
///
/// // Run the source with an emitter
/// source.run(emitter).await?;
/// ```
pub struct SocketTextSource {
    config: SocketConfig,
}

impl SocketTextSource {
    /// Create a new socket text source.
    pub fn new(config: SocketConfig) -> Self {
        Self { config }
    }

    /// Create a source in server mode on the specified port.
    pub fn server(port: u16) -> Self {
        Self::new(SocketConfig::server(port))
    }

    /// Create a source in client mode connecting to the specified host and port.
    pub fn client(host: impl Into<String>, port: u16) -> Self {
        Self::new(SocketConfig::client(host, port))
    }

    /// Run the source, emitting lines to the downstream.
    pub async fn run(&mut self, emitter: Emitter<String>) -> Result<()> {
        match self.config.mode {
            SocketMode::Server => self.run_server(emitter).await,
            SocketMode::Client => self.run_client(emitter).await,
        }
    }

    /// Run in server mode: bind port and accept connections.
    async fn run_server(&self, emitter: Emitter<String>) -> Result<()> {
        let addr = self.config.address();
        let listener = TcpListener::bind(&addr).await?;

        info!(address = %addr, "Socket source listening");

        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    info!(peer = %peer, "Client connected");

                    let emitter = emitter.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, emitter).await {
                            warn!(peer = %peer, error = %e, "Connection error");
                        }
                        info!(peer = %peer, "Client disconnected");
                    });
                }
                Err(e) => {
                    error!(error = %e, "Accept error");
                }
            }
        }
    }

    /// Run in client mode: connect to server and read lines.
    async fn run_client(&self, emitter: Emitter<String>) -> Result<()> {
        let addr = self.config.address();

        info!(address = %addr, "Socket source connecting");

        let timeout = Duration::from_millis(self.config.connect_timeout_ms);
        let stream = tokio::time::timeout(timeout, TcpStream::connect(&addr)).await??;

        info!(address = %addr, "Connected to server");

        handle_connection(stream, emitter).await?;

        info!(address = %addr, "Disconnected from server");
        Ok(())
    }
}

/// Handle a single TCP connection, reading lines and emitting them.
async fn handle_connection(stream: TcpStream, mut emitter: Emitter<String>) -> Result<()> {
    let reader = BufReader::new(stream);
    let mut lines = reader.lines();

    while let Some(line) = lines.next_line().await? {
        debug!(line = %line, "Received line");

        if let Err(e) = emitter.data(line).await {
            error!(error = %e, "Failed to emit event");
            break;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config() {
        let config = SocketConfig::server(9999);
        assert_eq!(config.port, 9999);
        assert_eq!(config.mode, SocketMode::Server);

        let config = SocketConfig::client("localhost", 8888);
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 8888);
        assert_eq!(config.mode, SocketMode::Client);
    }
}
