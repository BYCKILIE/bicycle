//! Connectors for external data sources and sinks.
//!
//! This crate provides source and sink connectors for popular message
//! queues and data systems.
//!
//! ## Available Connectors
//!
//! - **Kafka** (enabled by default): Apache Kafka source and sink with exactly-once support
//! - **Socket** (enabled by default): TCP socket source and sink for testing
//! - **Pulsar** (optional): Apache Pulsar source and sink
//!
//! ## Feature Flags
//!
//! - `kafka`: Enable Kafka connector (default)
//! - `socket`: Enable Socket connector for testing (default)
//! - `pulsar`: Enable Pulsar connector
//!
//! ## Example
//!
//! ```ignore
//! use bicycle_connectors::kafka::{KafkaSource, KafkaConfig};
//!
//! let config = KafkaConfig::new("localhost:9092")
//!     .with_group_id("my-consumer-group")
//!     .with_topics(vec!["my-topic".to_string()]);
//!
//! let mut source = KafkaSource::new(config);
//! source.connect()?;
//! ```

#[cfg(feature = "kafka")]
pub mod kafka;

#[cfg(feature = "socket")]
pub mod socket;

#[cfg(feature = "pulsar")]
pub mod pulsar;
