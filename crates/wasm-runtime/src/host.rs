//! Host functions exposed to WASM modules.
//!
//! These functions are callable from WASM code and provide access to:
//! - Emitting data to downstream operators
//! - Keyed state access
//! - Logging

use crate::{Result, WasmError};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info, trace, warn};

/// Host functions context passed to WASM modules.
///
/// This struct holds the state and callbacks that WASM code can use.
pub struct HostFunctions {
    /// Current operator name.
    operator_name: String,
    /// Current key (for keyed streams).
    current_key: RwLock<Option<Vec<u8>>>,
    /// State backend.
    state: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    /// Output buffer for emitted data.
    output_buffer: Arc<RwLock<Vec<Vec<u8>>>>,
    /// Callback for emitting data (optional, for async emit).
    emit_callback: Option<Box<dyn Fn(Vec<u8>) + Send + Sync>>,
}

impl HostFunctions {
    /// Create new host functions.
    pub fn new(operator_name: impl Into<String>) -> Self {
        Self {
            operator_name: operator_name.into(),
            current_key: RwLock::new(None),
            state: Arc::new(RwLock::new(HashMap::new())),
            output_buffer: Arc::new(RwLock::new(Vec::new())),
            emit_callback: None,
        }
    }

    /// Set the emit callback.
    pub fn with_emit_callback<F>(mut self, callback: F) -> Self
    where
        F: Fn(Vec<u8>) + Send + Sync + 'static,
    {
        self.emit_callback = Some(Box::new(callback));
        self
    }

    /// Set the state backend.
    pub fn with_state(mut self, state: Arc<RwLock<HashMap<String, Vec<u8>>>>) -> Self {
        self.state = state;
        self
    }

    /// Set the current key.
    pub fn set_current_key(&self, key: Option<Vec<u8>>) {
        *self.current_key.write() = key;
    }

    /// Get the current key.
    pub fn current_key(&self) -> Option<Vec<u8>> {
        self.current_key.read().clone()
    }

    /// Emit data to downstream operators.
    pub fn emit(&self, data: Vec<u8>) {
        trace!(
            operator = %self.operator_name,
            len = data.len(),
            "Emitting data"
        );

        if let Some(ref callback) = self.emit_callback {
            callback(data);
        } else {
            self.output_buffer.write().push(data);
        }
    }

    /// Get emitted data and clear the buffer.
    pub fn take_output(&self) -> Vec<Vec<u8>> {
        std::mem::take(&mut *self.output_buffer.write())
    }

    /// Get state value.
    pub fn get_state(&self, key: &str) -> Option<Vec<u8>> {
        let full_key = self.state_key(key);
        self.state.read().get(&full_key).cloned()
    }

    /// Set state value.
    pub fn set_state(&self, key: &str, value: Vec<u8>) {
        let full_key = self.state_key(key);
        self.state.write().insert(full_key, value);
    }

    /// Delete state value.
    pub fn delete_state(&self, key: &str) {
        let full_key = self.state_key(key);
        self.state.write().remove(&full_key);
    }

    /// Log a message.
    pub fn log(&self, level: LogLevel, message: &str) {
        match level {
            LogLevel::Trace => trace!(operator = %self.operator_name, "{}", message),
            LogLevel::Debug => debug!(operator = %self.operator_name, "{}", message),
            LogLevel::Info => info!(operator = %self.operator_name, "{}", message),
            LogLevel::Warn => warn!(operator = %self.operator_name, "{}", message),
            LogLevel::Error => error!(operator = %self.operator_name, "{}", message),
        }
    }

    /// Get all state (for checkpointing).
    pub fn snapshot_state(&self) -> HashMap<String, Vec<u8>> {
        self.state.read().clone()
    }

    /// Restore state (from checkpoint).
    pub fn restore_state(&self, state: HashMap<String, Vec<u8>>) {
        *self.state.write() = state;
    }

    /// Create the full state key including current key prefix.
    fn state_key(&self, key: &str) -> String {
        match self.current_key.read().as_ref() {
            Some(k) => {
                let key_hex: String = k.iter().map(|b| format!("{:02x}", b)).collect();
                format!("{}/{}", key_hex, key)
            }
            None => key.to_string(),
        }
    }
}

/// Log level for WASM logging.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LogLevel {
    Trace = 0,
    Debug = 1,
    Info = 2,
    Warn = 3,
    Error = 4,
}

impl From<u8> for LogLevel {
    fn from(v: u8) -> Self {
        match v {
            0 => LogLevel::Trace,
            1 => LogLevel::Debug,
            2 => LogLevel::Info,
            3 => LogLevel::Warn,
            _ => LogLevel::Error,
        }
    }
}

/// Host state for wasmtime linker.
///
/// This is the state passed to host functions via wasmtime's Store.
pub struct WasmHostState {
    /// Host functions.
    pub host: Arc<HostFunctions>,
    /// WASI context (for WASI support).
    pub wasi: wasmtime_wasi::WasiCtx,
}

impl WasmHostState {
    /// Create new host state.
    pub fn new(host: Arc<HostFunctions>) -> Self {
        let wasi = wasmtime_wasi::WasiCtxBuilder::new()
            .inherit_stdio()
            .build();

        Self { host, wasi }
    }
}

/// Create a linker with host functions.
pub fn create_linker(
    engine: &wasmtime::Engine,
) -> Result<wasmtime::Linker<WasmHostState>> {
    let mut linker = wasmtime::Linker::new(engine);

    // Add WASI support
    wasmtime_wasi::add_to_linker_sync(&mut linker, |state: &mut WasmHostState| &mut state.wasi)
        .map_err(|e| WasmError::Instantiation(e.to_string()))?;

    // Add bicycle host functions
    add_bicycle_functions(&mut linker)?;

    Ok(linker)
}

/// Add Bicycle-specific host functions to the linker.
fn add_bicycle_functions(
    linker: &mut wasmtime::Linker<WasmHostState>,
) -> Result<()> {
    // bicycle_emit(data_ptr: i32, data_len: i32)
    linker
        .func_wrap(
            "bicycle",
            "emit",
            |mut caller: wasmtime::Caller<'_, WasmHostState>, data_ptr: i32, data_len: i32| {
                let memory = caller.get_export("memory")
                    .and_then(|e| e.into_memory())
                    .ok_or_else(|| wasmtime::Error::msg("memory export not found"))?;

                let data = memory.data(&caller)
                    .get(data_ptr as usize..(data_ptr + data_len) as usize)
                    .ok_or_else(|| wasmtime::Error::msg("invalid memory access"))?
                    .to_vec();

                caller.data().host.emit(data);
                Ok(())
            },
        )
        .map_err(|e| WasmError::Instantiation(e.to_string()))?;

    // bicycle_get_state(key_ptr: i32, key_len: i32, out_ptr: i32, out_cap: i32) -> i32 (actual len, or -1 if not found)
    linker
        .func_wrap(
            "bicycle",
            "get_state",
            |mut caller: wasmtime::Caller<'_, WasmHostState>,
             key_ptr: i32,
             key_len: i32,
             out_ptr: i32,
             out_cap: i32| -> i32 {
                let memory = match caller.get_export("memory").and_then(|e| e.into_memory()) {
                    Some(m) => m,
                    None => return -1,
                };

                let key = match memory
                    .data(&caller)
                    .get(key_ptr as usize..(key_ptr + key_len) as usize)
                {
                    Some(k) => String::from_utf8_lossy(k).to_string(),
                    None => return -1,
                };

                match caller.data().host.get_state(&key) {
                    Some(value) => {
                        let len = value.len() as i32;
                        if len <= out_cap {
                            let mem_data = memory.data_mut(&mut caller);
                            mem_data[out_ptr as usize..(out_ptr as usize + value.len())]
                                .copy_from_slice(&value);
                        }
                        len
                    }
                    None => -1,
                }
            },
        )
        .map_err(|e| WasmError::Instantiation(e.to_string()))?;

    // bicycle_set_state(key_ptr: i32, key_len: i32, val_ptr: i32, val_len: i32)
    linker
        .func_wrap(
            "bicycle",
            "set_state",
            |mut caller: wasmtime::Caller<'_, WasmHostState>,
             key_ptr: i32,
             key_len: i32,
             val_ptr: i32,
             val_len: i32| {
                let memory = caller
                    .get_export("memory")
                    .and_then(|e| e.into_memory())
                    .ok_or_else(|| wasmtime::Error::msg("memory export not found"))?;

                let mem_data = memory.data(&caller);

                let key = mem_data
                    .get(key_ptr as usize..(key_ptr + key_len) as usize)
                    .map(|k| String::from_utf8_lossy(k).to_string())
                    .ok_or_else(|| wasmtime::Error::msg("invalid key memory access"))?;

                let value = mem_data
                    .get(val_ptr as usize..(val_ptr + val_len) as usize)
                    .map(|v| v.to_vec())
                    .ok_or_else(|| wasmtime::Error::msg("invalid value memory access"))?;

                caller.data().host.set_state(&key, value);
                Ok(())
            },
        )
        .map_err(|e| WasmError::Instantiation(e.to_string()))?;

    // bicycle_log(level: i32, msg_ptr: i32, msg_len: i32)
    linker
        .func_wrap(
            "bicycle",
            "log",
            |mut caller: wasmtime::Caller<'_, WasmHostState>,
             level: i32,
             msg_ptr: i32,
             msg_len: i32| {
                let memory = caller
                    .get_export("memory")
                    .and_then(|e| e.into_memory())
                    .ok_or_else(|| wasmtime::Error::msg("memory export not found"))?;

                let msg = memory
                    .data(&caller)
                    .get(msg_ptr as usize..(msg_ptr + msg_len) as usize)
                    .map(|m| String::from_utf8_lossy(m).to_string())
                    .ok_or_else(|| wasmtime::Error::msg("invalid message memory access"))?;

                caller.data().host.log(LogLevel::from(level as u8), &msg);
                Ok(())
            },
        )
        .map_err(|e| WasmError::Instantiation(e.to_string()))?;

    // bicycle_get_current_key(out_ptr: i32, out_cap: i32) -> i32 (actual len, or -1 if no key)
    linker
        .func_wrap(
            "bicycle",
            "get_current_key",
            |mut caller: wasmtime::Caller<'_, WasmHostState>, out_ptr: i32, out_cap: i32| -> i32 {
                let memory = match caller.get_export("memory").and_then(|e| e.into_memory()) {
                    Some(m) => m,
                    None => return -1,
                };

                match caller.data().host.current_key() {
                    Some(key) => {
                        let len = key.len() as i32;
                        if len <= out_cap {
                            let mem_data = memory.data_mut(&mut caller);
                            mem_data[out_ptr as usize..(out_ptr as usize + key.len())]
                                .copy_from_slice(&key);
                        }
                        len
                    }
                    None => -1,
                }
            },
        )
        .map_err(|e| WasmError::Instantiation(e.to_string()))?;

    Ok(())
}
