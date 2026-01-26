//! Prelude module - common imports for Bicycle jobs.
//!
//! Import this module to get all commonly used types:
//!
//! ```ignore
//! use bicycle_api::prelude::*;
//! ```

// Core traits
pub use crate::function::{
    AggregateFunction, AsyncFunction, FilterFunction, FlatMapFunction, KeySelector, MapFunction,
    ReduceFunction, RichAsyncFunction,
};

// Context types
pub use crate::context::{Context, MemoryStateBackend, RuntimeContext, StateBackend, StateBackendExt, TaskInfo};

// State types
pub use crate::state::{ListState, MapState, ValueState};

// Stream types
pub use crate::datastream::{DataStream, KeyedStream, WindowedStream};

// Environment
pub use crate::environment::{StreamEnvBuilder, StreamEnvironment};

// Graph types (for advanced use)
pub use crate::graph::{
    ConnectorConfig, ConnectorType, Edge, JobConfig, JobGraph, OperatorType, PartitionStrategy,
    Vertex, WindowType,
};

// Error and Result
pub use crate::{Error, Result};

// Re-exports
pub use async_trait::async_trait;
pub use serde::{de::DeserializeOwned, Deserialize, Serialize};

// Common std types used in streaming jobs
pub use std::collections::HashMap;
pub use std::sync::Arc;
