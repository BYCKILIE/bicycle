//! Configuration for socket connectors.

/// Mode of operation for socket connectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SocketMode {
    /// Act as a TCP server: bind to a port and accept connections.
    Server,
    /// Act as a TCP client: connect to a remote server.
    Client,
}

impl Default for SocketMode {
    fn default() -> Self {
        Self::Server
    }
}

/// Configuration for socket connectors.
#[derive(Debug, Clone)]
pub struct SocketConfig {
    /// Host to bind/connect to.
    pub host: String,
    /// Port to bind/connect to.
    pub port: u16,
    /// Connection mode (server or client).
    pub mode: SocketMode,
    /// Maximum number of connections (server mode only).
    pub max_connections: usize,
    /// Connection timeout in milliseconds.
    pub connect_timeout_ms: u64,
    /// Read timeout in milliseconds (0 = no timeout).
    pub read_timeout_ms: u64,
}

impl Default for SocketConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 9999,
            mode: SocketMode::Server,
            max_connections: 10,
            connect_timeout_ms: 5000,
            read_timeout_ms: 0,
        }
    }
}

impl SocketConfig {
    /// Create a new socket configuration.
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
            ..Default::default()
        }
    }

    /// Create a server configuration that binds to a port.
    pub fn server(port: u16) -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port,
            mode: SocketMode::Server,
            ..Default::default()
        }
    }

    /// Create a client configuration that connects to a server.
    pub fn client(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
            mode: SocketMode::Client,
            ..Default::default()
        }
    }

    /// Set the connection mode.
    pub fn with_mode(mut self, mode: SocketMode) -> Self {
        self.mode = mode;
        self
    }

    /// Set maximum connections (server mode only).
    pub fn with_max_connections(mut self, max: usize) -> Self {
        self.max_connections = max;
        self
    }

    /// Set connection timeout.
    pub fn with_connect_timeout(mut self, timeout_ms: u64) -> Self {
        self.connect_timeout_ms = timeout_ms;
        self
    }

    /// Set read timeout.
    pub fn with_read_timeout(mut self, timeout_ms: u64) -> Self {
        self.read_timeout_ms = timeout_ms;
        self
    }

    /// Get the socket address string.
    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}
