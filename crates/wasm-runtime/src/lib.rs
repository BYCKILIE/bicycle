//! WASM Runtime for Bicycle
//!
//! This crate provides the WASM execution environment for running
//! user-defined streaming functions on workers.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │                      Worker                              │
//! │  ┌───────────────────────────────────────────────────┐  │
//! │  │                 WasmExecutor                       │  │
//! │  │  ┌─────────────┐  ┌─────────────┐  ┌───────────┐  │  │
//! │  │  │   Engine    │  │   Module    │  │  Instance │  │  │
//! │  │  │  (shared)   │  │   Cache     │  │   Pool    │  │  │
//! │  │  └─────────────┘  └─────────────┘  └───────────┘  │  │
//! │  │                                                    │  │
//! │  │  Host Functions:                                   │  │
//! │  │  - emit(data)     -> send to downstream            │  │
//! │  │  - get_state(key) -> read keyed state              │  │
//! │  │  - set_state(key, value) -> write keyed state      │  │
//! │  │  - log(level, msg) -> logging                      │  │
//! │  └───────────────────────────────────────────────────┘  │
//! └─────────────────────────────────────────────────────────┘
//! ```

pub mod executor;
pub mod host;
pub mod module;

pub use executor::WasmExecutor;
pub use host::HostFunctions;
pub use module::{CompiledModule, ModuleCache};

use thiserror::Error;

/// Errors that can occur in the WASM runtime.
#[derive(Debug, Error)]
pub enum WasmError {
    #[error("Failed to compile WASM module: {0}")]
    Compilation(String),

    #[error("Failed to instantiate WASM module: {0}")]
    Instantiation(String),

    #[error("Failed to call WASM function: {0}")]
    Execution(String),

    #[error("WASM function not found: {0}")]
    FunctionNotFound(String),

    #[error("Invalid WASM module: {0}")]
    InvalidModule(String),

    #[error("Host function error: {0}")]
    HostFunction(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

impl From<anyhow::Error> for WasmError {
    fn from(e: anyhow::Error) -> Self {
        WasmError::Execution(e.to_string())
    }
}

/// Result type for WASM operations.
pub type Result<T> = std::result::Result<T, WasmError>;
