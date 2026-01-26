//! WASM Module loading and caching.

use crate::{Result, WasmError};
use bicycle_api::graph::JobGraph;
use dashmap::DashMap;
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, info};
use wasmtime::{Engine, Module};

/// A compiled WASM module ready for instantiation.
pub struct CompiledModule {
    /// The wasmtime module.
    module: Module,
    /// The original WASM bytes (for re-distribution).
    wasm_bytes: Vec<u8>,
    /// Extracted job graph (if available).
    job_graph: Option<JobGraph>,
}

impl CompiledModule {
    /// Compile a module from WASM bytes.
    pub fn from_bytes(engine: &Engine, wasm_bytes: Vec<u8>) -> Result<Self> {
        let module = Module::new(engine, &wasm_bytes)
            .map_err(|e| WasmError::Compilation(e.to_string()))?;

        Ok(Self {
            module,
            wasm_bytes,
            job_graph: None,
        })
    }

    /// Load and compile a module from a file.
    pub fn from_file(engine: &Engine, path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        info!(path = %path.display(), "Loading WASM module from file");

        let wasm_bytes = std::fs::read(path)?;
        Self::from_bytes(engine, wasm_bytes)
    }

    /// Get the wasmtime module.
    pub fn module(&self) -> &Module {
        &self.module
    }

    /// Get the original WASM bytes.
    pub fn wasm_bytes(&self) -> &[u8] {
        &self.wasm_bytes
    }

    /// Get the job graph if extracted.
    pub fn job_graph(&self) -> Option<&JobGraph> {
        self.job_graph.as_ref()
    }

    /// Set the job graph.
    pub fn set_job_graph(&mut self, graph: JobGraph) {
        self.job_graph = Some(graph);
    }
}

/// Cache for compiled WASM modules.
///
/// Modules are cached by their hash to avoid recompilation.
pub struct ModuleCache {
    /// The wasmtime engine (shared across all modules).
    engine: Engine,
    /// Cache of compiled modules by hash.
    cache: DashMap<String, Arc<CompiledModule>>,
}

impl ModuleCache {
    /// Create a new module cache.
    pub fn new() -> Result<Self> {
        let engine = Engine::default();
        Ok(Self {
            engine,
            cache: DashMap::new(),
        })
    }

    /// Create a module cache with a custom engine.
    pub fn with_engine(engine: Engine) -> Self {
        Self {
            engine,
            cache: DashMap::new(),
        }
    }

    /// Get the engine.
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Get or compile a module from bytes.
    pub fn get_or_compile(&self, wasm_bytes: Vec<u8>) -> Result<Arc<CompiledModule>> {
        let hash = hash_bytes(&wasm_bytes);

        if let Some(module) = self.cache.get(&hash) {
            debug!(hash = %hash, "Module cache hit");
            return Ok(module.clone());
        }

        debug!(hash = %hash, "Module cache miss, compiling");
        let module = Arc::new(CompiledModule::from_bytes(&self.engine, wasm_bytes)?);
        self.cache.insert(hash, module.clone());

        Ok(module)
    }

    /// Get or load a module from a file.
    pub fn get_or_load(&self, path: impl AsRef<Path>) -> Result<Arc<CompiledModule>> {
        let path = path.as_ref();
        let wasm_bytes = std::fs::read(path)?;
        self.get_or_compile(wasm_bytes)
    }

    /// Remove a module from the cache.
    pub fn remove(&self, wasm_bytes: &[u8]) {
        let hash = hash_bytes(wasm_bytes);
        self.cache.remove(&hash);
    }

    /// Clear all cached modules.
    pub fn clear(&self) {
        self.cache.clear();
    }

    /// Get the number of cached modules.
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }
}

impl Default for ModuleCache {
    fn default() -> Self {
        Self::new().expect("Failed to create default module cache")
    }
}

/// Hash bytes using a simple algorithm.
fn hash_bytes(data: &[u8]) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    data.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_bytes() {
        let data1 = b"hello world";
        let data2 = b"hello world";
        let data3 = b"different data";

        assert_eq!(hash_bytes(data1), hash_bytes(data2));
        assert_ne!(hash_bytes(data1), hash_bytes(data3));
    }
}
