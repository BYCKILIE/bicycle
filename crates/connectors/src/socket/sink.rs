//! Socket text sink connector.
//!
//! Writes events as lines to TCP connections.

use super::config::{SocketConfig, SocketMode};
use anyhow::Result;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, error, info, warn};

/// A socket sink that writes lines to TCP connections.
///
/// # Modes
///
/// - **Server mode**: Binds to a port, accepts connections, and broadcasts
///   all events to connected clients.
/// - **Client mode**: Connects to a server and writes events to it.
///
/// # Delivery Semantics
///
/// This sink provides **best-effort** delivery only:
/// - No transactions or exactly-once guarantees
/// - Messages may be lost if clients disconnect
/// - Use Kafka sink for production workloads
///
/// # Example
///
/// ```ignore
/// use bicycle_connectors::socket::{SocketTextSink, SocketConfig};
///
/// let config = SocketConfig::server(9998);
/// let sink = SocketTextSink::new(config);
/// sink.start().await?;
///
/// // Write lines
/// sink.write("hello world").await?;
/// ```
pub struct SocketTextSink {
    config: SocketConfig,
    /// Broadcast channel for sending messages to connected clients (server mode).
    tx: broadcast::Sender<String>,
    /// Counter for connected clients.
    client_count: Arc<AtomicU64>,
    /// Direct connection for client mode.
    connection: Arc<RwLock<Option<TcpStream>>>,
    /// Whether the sink has been started.
    started: Arc<RwLock<bool>>,
}

impl SocketTextSink {
    /// Create a new socket text sink.
    pub fn new(config: SocketConfig) -> Self {
        let (tx, _) = broadcast::channel(1024);

        Self {
            config,
            tx,
            client_count: Arc::new(AtomicU64::new(0)),
            connection: Arc::new(RwLock::new(None)),
            started: Arc::new(RwLock::new(false)),
        }
    }

    /// Create a sink in server mode on the specified port.
    pub fn server(port: u16) -> Self {
        Self::new(SocketConfig::server(port))
    }

    /// Create a sink in client mode connecting to the specified host and port.
    pub fn client(host: impl Into<String>, port: u16) -> Self {
        Self::new(SocketConfig::client(host, port))
    }

    /// Start the sink.
    pub async fn start(&self) -> Result<()> {
        let mut started = self.started.write().await;
        if *started {
            return Ok(());
        }

        match self.config.mode {
            SocketMode::Server => self.start_server().await?,
            SocketMode::Client => self.start_client().await?,
        }

        *started = true;
        Ok(())
    }

    /// Start in server mode: bind port and accept connections.
    async fn start_server(&self) -> Result<()> {
        let addr = self.config.address();
        let listener = TcpListener::bind(&addr).await?;

        info!(address = %addr, "Socket sink listening");

        let tx = self.tx.clone();
        let client_count = self.client_count.clone();

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, peer)) => {
                        info!(peer = %peer, "Client connected to sink");
                        client_count.fetch_add(1, Ordering::Relaxed);

                        let mut rx = tx.subscribe();
                        let client_count = client_count.clone();

                        tokio::spawn(async move {
                            let mut stream = stream;

                            loop {
                                match rx.recv().await {
                                    Ok(line) => {
                                        let data = format!("{}\n", line);
                                        if let Err(e) = stream.write_all(data.as_bytes()).await {
                                            warn!(peer = %peer, error = %e, "Write error");
                                            break;
                                        }
                                    }
                                    Err(broadcast::error::RecvError::Closed) => break,
                                    Err(broadcast::error::RecvError::Lagged(n)) => {
                                        warn!(peer = %peer, skipped = n, "Client lagging");
                                    }
                                }
                            }

                            client_count.fetch_sub(1, Ordering::Relaxed);
                            info!(peer = %peer, "Client disconnected from sink");
                        });
                    }
                    Err(e) => {
                        error!(error = %e, "Accept error in sink");
                    }
                }
            }
        });

        Ok(())
    }

    /// Start in client mode: connect to server.
    async fn start_client(&self) -> Result<()> {
        let addr = self.config.address();

        info!(address = %addr, "Socket sink connecting");

        let timeout = Duration::from_millis(self.config.connect_timeout_ms);
        let stream = tokio::time::timeout(timeout, TcpStream::connect(&addr)).await??;

        info!(address = %addr, "Connected to server");

        *self.connection.write().await = Some(stream);
        Ok(())
    }

    /// Write a line to the sink.
    pub async fn write(&self, line: impl AsRef<str>) -> Result<()> {
        let line = line.as_ref();
        debug!(line = %line, "Writing line");

        match self.config.mode {
            SocketMode::Server => {
                // Broadcast to all connected clients
                let _ = self.tx.send(line.to_string());
            }
            SocketMode::Client => {
                // Write directly to the connection
                let mut conn = self.connection.write().await;
                if let Some(stream) = conn.as_mut() {
                    let data = format!("{}\n", line);
                    stream.write_all(data.as_bytes()).await?;
                } else {
                    anyhow::bail!("Not connected");
                }
            }
        }

        Ok(())
    }

    /// Write multiple lines to the sink.
    pub async fn write_all(&self, lines: impl IntoIterator<Item = impl AsRef<str>>) -> Result<()> {
        for line in lines {
            self.write(line).await?;
        }
        Ok(())
    }

    /// Get the number of connected clients (server mode only).
    pub fn client_count(&self) -> u64 {
        self.client_count.load(Ordering::Relaxed)
    }

    /// Flush pending writes.
    pub async fn flush(&self) -> Result<()> {
        if self.config.mode == SocketMode::Client {
            let mut conn = self.connection.write().await;
            if let Some(stream) = conn.as_mut() {
                stream.flush().await?;
            }
        }
        Ok(())
    }

    /// Close the sink.
    pub async fn close(&self) -> Result<()> {
        if self.config.mode == SocketMode::Client {
            let mut conn = self.connection.write().await;
            if let Some(stream) = conn.take() {
                drop(stream);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sink_creation() {
        let sink = SocketTextSink::server(9998);
        assert_eq!(sink.config.port, 9998);
        assert_eq!(sink.config.mode, SocketMode::Server);

        let sink = SocketTextSink::client("localhost", 8888);
        assert_eq!(sink.config.host, "localhost");
        assert_eq!(sink.config.port, 8888);
        assert_eq!(sink.config.mode, SocketMode::Client);
    }
}
