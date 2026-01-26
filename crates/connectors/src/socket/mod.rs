//! Socket-based connectors for testing and development.
//!
//! # WARNING: TEST-ONLY
//!
//! These connectors are intended for testing and development purposes only.
//! They are NOT suitable for production use because:
//! - No fault tolerance (connections are not persisted)
//! - No exactly-once guarantees
//! - Limited scalability
//!
//! For production streaming, use Kafka or other durable message queues.
//!
//! # Usage
//!
//! ## Socket Source
//!
//! Read lines from a TCP connection:
//!
//! ```bash
//! # Start source listening on port 9999
//! # Then use netcat to send data:
//! nc localhost 9999
//! hello world
//! foo bar
//! ```
//!
//! ## Socket Sink
//!
//! Write events as lines to a TCP connection:
//!
//! ```bash
//! # Start sink listening on port 9998
//! # Then use netcat to receive data:
//! nc localhost 9998
//! ```

mod config;
mod sink;
mod source;

pub use config::{SocketConfig, SocketMode};
pub use sink::SocketTextSink;
pub use source::SocketTextSource;
