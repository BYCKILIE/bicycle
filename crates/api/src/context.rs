//! Context types for function execution.
//!
//! This module provides the context objects that are passed to user functions,
//! similar to Flink's RuntimeContext.

use crate::state::{ListState, MapState, ValueState};
use crate::{Error, Result};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Basic context for stateless functions.
///
/// Provides access to task information and processing time.
#[derive(Debug, Clone)]
pub struct Context {
    /// Information about the current task.
    pub task_info: TaskInfo,
    /// Current processing time (system time).
    pub processing_time: u64,
}

impl Context {
    /// Create a new context.
    pub fn new(task_info: TaskInfo) -> Self {
        Self {
            task_info,
            processing_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
        }
    }

    /// Create a default context for standalone jobs.
    pub fn standalone() -> Self {
        Self::new(TaskInfo::default())
    }

    /// Get the current processing time.
    pub fn current_processing_time(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(self.processing_time)
    }
}

/// Runtime context for stateful functions (RichAsyncFunction).
///
/// Provides access to:
/// - Task information
/// - Keyed state (value, list, map states)
/// - Checkpointing
/// - Timers (future)
#[derive(Clone)]
pub struct RuntimeContext {
    /// Information about the current task.
    pub task_info: TaskInfo,
    /// Current watermark (event time progress).
    pub watermark: u64,
    /// Current processing time.
    pub processing_time: u64,
    /// State backend for keyed state access.
    state_backend: Arc<dyn StateBackend>,
    /// Current key (for keyed streams).
    current_key: Option<Vec<u8>>,
}

impl RuntimeContext {
    /// Create a new runtime context.
    pub fn new(task_info: TaskInfo, state_backend: Arc<dyn StateBackend>) -> Self {
        Self {
            task_info,
            watermark: 0,
            processing_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            state_backend,
            current_key: None,
        }
    }

    /// Create a default runtime context with in-memory state for standalone jobs.
    pub fn standalone() -> Self {
        Self::new(TaskInfo::default(), Arc::new(MemoryStateBackend::new()))
    }

    /// Set the current key (called by the runtime before processing keyed elements).
    pub fn set_current_key(&mut self, key: Vec<u8>) {
        self.current_key = Some(key);
    }

    /// Clear the current key.
    pub fn clear_current_key(&mut self) {
        self.current_key = None;
    }

    /// Get the current key.
    pub fn current_key(&self) -> Option<&[u8]> {
        self.current_key.as_deref()
    }

    /// Get value state by name.
    ///
    /// Value state stores a single value per key.
    pub fn get_state<T: Serialize + DeserializeOwned>(&self, name: &str) -> Result<Option<T>> {
        let key = self.state_key(name);
        match self.state_backend.get_bytes(&key)? {
            Some(data) => {
                let value: T = serde_json::from_slice(&data).map_err(|e| Error::Serialization(e.to_string()))?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Save value state.
    pub fn save_state<T: Serialize>(&self, name: &str, value: &T) -> Result<()> {
        let key = self.state_key(name);
        let data = serde_json::to_vec(value).map_err(|e| Error::Serialization(e.to_string()))?;
        self.state_backend.put_bytes(&key, data)
    }

    /// Delete value state.
    pub fn delete_state(&self, name: &str) -> Result<()> {
        let key = self.state_key(name);
        self.state_backend.delete(&key)
    }

    /// Get a value state descriptor.
    pub fn get_value_state<T: Serialize + DeserializeOwned + Default + Clone + Send + Sync + 'static>(
        &self,
        name: &str,
    ) -> ValueState<T> {
        ValueState::new(name.to_string(), self.state_backend.clone(), self.current_key.clone())
    }

    /// Get a list state descriptor.
    pub fn get_list_state<T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static>(
        &self,
        name: &str,
    ) -> ListState<T> {
        ListState::new(name.to_string(), self.state_backend.clone(), self.current_key.clone())
    }

    /// Get a map state descriptor.
    pub fn get_map_state<K, V>(&self, name: &str) -> MapState<K, V>
    where
        K: Serialize + DeserializeOwned + Clone + std::hash::Hash + Eq + Send + Sync + 'static,
        V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    {
        MapState::new(name.to_string(), self.state_backend.clone(), self.current_key.clone())
    }

    /// Get the current watermark.
    pub fn current_watermark(&self) -> u64 {
        self.watermark
    }

    /// Get the current processing time.
    pub fn current_processing_time(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(self.processing_time)
    }

    /// Create the state key including the current key if present.
    fn state_key(&self, name: &str) -> String {
        match &self.current_key {
            Some(key) => {
                let key_hex = hex::encode(key);
                format!("{}/{}", key_hex, name)
            }
            None => name.to_string(),
        }
    }
}

impl std::fmt::Debug for RuntimeContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeContext")
            .field("task_info", &self.task_info)
            .field("watermark", &self.watermark)
            .field("processing_time", &self.processing_time)
            .field("current_key", &self.current_key.as_ref().map(|k| hex::encode(k)))
            .finish()
    }
}

/// Information about the current task/operator.
#[derive(Debug, Clone, Default)]
pub struct TaskInfo {
    /// Job ID.
    pub job_id: String,
    /// Operator/vertex name.
    pub operator_name: String,
    /// Subtask index (0-based).
    pub subtask_index: u32,
    /// Total parallelism.
    pub parallelism: u32,
    /// Maximum parallelism (for state key assignment).
    pub max_parallelism: u32,
}

impl TaskInfo {
    /// Create new task info.
    pub fn new(
        job_id: impl Into<String>,
        operator_name: impl Into<String>,
        subtask_index: u32,
        parallelism: u32,
    ) -> Self {
        Self {
            job_id: job_id.into(),
            operator_name: operator_name.into(),
            subtask_index,
            parallelism,
            max_parallelism: parallelism.max(128),
        }
    }
}

/// State backend trait for keyed state storage.
///
/// This trait abstracts the state storage mechanism, allowing different
/// implementations (memory, RocksDB, etc.).
///
/// Note: Uses bytes for values to be object-safe (dyn compatible).
pub trait StateBackend: Send + Sync + 'static {
    /// Get raw bytes from state.
    fn get_bytes(&self, key: &str) -> Result<Option<Vec<u8>>>;

    /// Put raw bytes into state.
    fn put_bytes(&self, key: &str, value: Vec<u8>) -> Result<()>;

    /// Delete a value from state.
    fn delete(&self, key: &str) -> Result<()>;

    /// Get all keys with a given prefix.
    fn keys_with_prefix(&self, prefix: &str) -> Result<Vec<String>>;

    /// Snapshot state for checkpointing.
    fn snapshot(&self) -> Result<Vec<u8>>;

    /// Restore state from a snapshot.
    fn restore(&self, data: &[u8]) -> Result<()>;
}

/// Extension trait for StateBackend with typed access.
pub trait StateBackendExt: StateBackend {
    /// Get a typed value from state.
    fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        match self.get_bytes(key)? {
            Some(data) => {
                let value: T = serde_json::from_slice(&data).map_err(|e| Error::Serialization(e.to_string()))?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Put a typed value into state.
    fn put<T: Serialize>(&self, key: &str, value: &T) -> Result<()> {
        let data = serde_json::to_vec(value).map_err(|e| Error::Serialization(e.to_string()))?;
        self.put_bytes(key, data)
    }
}

// Blanket implementation
impl<T: StateBackend + ?Sized> StateBackendExt for T {}

/// In-memory state backend for testing.
#[derive(Default)]
pub struct MemoryStateBackend {
    state: std::sync::RwLock<HashMap<String, Vec<u8>>>,
}

impl MemoryStateBackend {
    pub fn new() -> Self {
        Self::default()
    }
}

impl StateBackend for MemoryStateBackend {
    fn get_bytes(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let state = self.state.read().map_err(|e| Error::State(e.to_string()))?;
        Ok(state.get(key).cloned())
    }

    fn put_bytes(&self, key: &str, value: Vec<u8>) -> Result<()> {
        let mut state = self.state.write().map_err(|e| Error::State(e.to_string()))?;
        state.insert(key.to_string(), value);
        Ok(())
    }

    fn delete(&self, key: &str) -> Result<()> {
        let mut state = self.state.write().map_err(|e| Error::State(e.to_string()))?;
        state.remove(key);
        Ok(())
    }

    fn keys_with_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let state = self.state.read().map_err(|e| Error::State(e.to_string()))?;
        Ok(state
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }

    fn snapshot(&self) -> Result<Vec<u8>> {
        let state = self.state.read().map_err(|e| Error::State(e.to_string()))?;
        let data = serde_json::to_vec(&*state)?;
        Ok(data)
    }

    fn restore(&self, data: &[u8]) -> Result<()> {
        let restored: HashMap<String, Vec<u8>> = serde_json::from_slice(data)?;
        let mut state = self.state.write().map_err(|e| Error::State(e.to_string()))?;
        *state = restored;
        Ok(())
    }
}

// Hex encoding helper (avoid extra dependency)
mod hex {
    pub fn encode(data: &[u8]) -> String {
        data.iter().map(|b| format!("{:02x}", b)).collect()
    }
}
