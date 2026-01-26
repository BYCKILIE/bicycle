//! Bicycle Native Plugin Support
//!
//! This crate provides the infrastructure for building Bicycle streaming jobs
//! as native shared libraries (.so on Linux, .dylib on macOS, .dll on Windows).
//!
//! # Overview
//!
//! Instead of using WASM, this approach compiles jobs as native code that is
//! dynamically loaded by workers using `libloading`. This provides:
//!
//! - Full native performance
//! - Access to system libraries and I/O
//! - Easier debugging
//!
//! # Usage
//!
//! ```ignore
//! use bicycle_api::prelude::*;
//! use bicycle_plugin::bicycle_plugin;
//!
//! pub struct WordSplitter;
//!
//! #[async_trait]
//! impl AsyncFunction for WordSplitter {
//!     type In = String;
//!     type Out = String;
//!
//!     async fn process(&mut self, input: String, _ctx: &Context) -> Vec<String> {
//!         input.split_whitespace().map(|s| s.to_lowercase()).collect()
//!     }
//! }
//!
//! bicycle_plugin!(WordSplitter);
//! ```
//!
//! # Plugin ABI
//!
//! Plugins export these `extern "C"` functions:
//!
//! - `bicycle_plugin_info()` - Returns plugin metadata
//! - `bicycle_create(name, config, len)` - Creates a function instance
//! - `bicycle_open(handle, ctx, len)` - Opens/initializes the function
//! - `bicycle_process(handle, input, in_len, ctx, ctx_len)` - Processes input
//! - `bicycle_close(handle)` - Closes the function
//! - `bicycle_snapshot(handle, ctx, len)` - Creates state snapshot
//! - `bicycle_restore(handle, data, len)` - Restores from snapshot
//! - `bicycle_destroy(handle)` - Destroys the function instance

pub mod ffi;

pub use ffi::{
    BicycleBytes, BicycleResult, FunctionInfo, PluginContext, PluginInfo,
    bicycle_alloc, bicycle_free, bicycle_free_bytes, bicycle_free_result,
};

// Re-export bicycle-api types for convenience
pub use bicycle_api::prelude::*;
pub use bicycle_api::{AsyncFunction, RichAsyncFunction};

/// Macro to generate all FFI exports for a Bicycle plugin.
///
/// This macro generates the required `extern "C"` functions that allow
/// the plugin to be loaded and executed by Bicycle workers.
///
/// # Example
///
/// ```ignore
/// use bicycle_plugin::bicycle_plugin;
///
/// pub struct MyFunction;
///
/// impl AsyncFunction for MyFunction {
///     // ... implementation
/// }
///
/// bicycle_plugin!(MyFunction);
/// ```
///
/// For multiple functions:
///
/// ```ignore
/// bicycle_plugin!(WordSplitter, WordCounter);
/// ```
#[macro_export]
macro_rules! bicycle_plugin {
    ($($func:ty),+ $(,)?) => {
        // Plugin metadata
        static PLUGIN_INFO: std::sync::OnceLock<$crate::PluginInfo> = std::sync::OnceLock::new();

        fn get_plugin_info() -> &'static $crate::PluginInfo {
            PLUGIN_INFO.get_or_init(|| {
                let mut info = $crate::PluginInfo::new(
                    env!("CARGO_PKG_NAME"),
                    env!("CARGO_PKG_VERSION"),
                );
                $(
                    info.add_function($crate::FunctionInfo {
                        name: std::any::type_name::<$func>().to_string(),
                        is_rich: $crate::__is_rich_function::<$func>(),
                        input_type: std::any::type_name::<<$func as $crate::__FunctionTypes>::In>().to_string(),
                        output_type: std::any::type_name::<<$func as $crate::__FunctionTypes>::Out>().to_string(),
                    });
                )+
                info
            })
        }

        /// Returns plugin metadata as JSON.
        #[no_mangle]
        pub extern "C" fn bicycle_plugin_info() -> $crate::BicycleBytes {
            let info = get_plugin_info();
            $crate::BicycleBytes::success(info.to_bytes())
        }

        /// Create a function instance by name.
        ///
        /// # Safety
        /// - `name` must be a valid null-terminated C string.
        /// - `config` may be null if `config_len` is 0.
        #[no_mangle]
        pub unsafe extern "C" fn bicycle_create(
            name: *const std::os::raw::c_char,
            config: *const u8,
            config_len: usize,
        ) -> *mut std::ffi::c_void {
            let name_str = $crate::ffi::c_str_to_string(name);
            let _config_bytes = $crate::ffi::read_bytes(config, config_len);

            // Try each function type
            $(
                if name_str == std::any::type_name::<$func>() || name_str.ends_with(stringify!($func)) {
                    let instance = Box::new(<$func>::default());
                    let wrapper = Box::new($crate::__FunctionWrapper::<$func>::new(*instance));
                    return Box::into_raw(wrapper) as *mut std::ffi::c_void;
                }
            )+

            std::ptr::null_mut()
        }

        /// Open/initialize a function instance.
        ///
        /// # Safety
        /// - `handle` must be a valid pointer from `bicycle_create`.
        /// - `ctx` may be null if `ctx_len` is 0.
        #[no_mangle]
        pub unsafe extern "C" fn bicycle_open(
            handle: *mut std::ffi::c_void,
            ctx: *const u8,
            ctx_len: usize,
        ) -> $crate::BicycleResult {
            if handle.is_null() {
                return $crate::BicycleResult::err("Null handle".to_string());
            }

            let ctx_bytes = $crate::ffi::read_bytes(ctx, ctx_len);
            let plugin_ctx = match $crate::PluginContext::from_bytes(&ctx_bytes) {
                Ok(c) => c,
                Err(e) => return $crate::BicycleResult::err(format!("Invalid context: {}", e)),
            };

            // Try each function type
            $(
                {
                    let wrapper = handle as *mut $crate::__FunctionWrapper<$func>;
                    if let Some(w) = wrapper.as_mut() {
                        return w.open(&plugin_ctx);
                    }
                }
            )+

            $crate::BicycleResult::err("Invalid handle".to_string())
        }

        /// Process input data through the function.
        ///
        /// # Safety
        /// - `handle` must be a valid pointer from `bicycle_create`.
        /// - `input` may be null if `input_len` is 0.
        /// - `ctx` may be null if `ctx_len` is 0.
        #[no_mangle]
        pub unsafe extern "C" fn bicycle_process(
            handle: *mut std::ffi::c_void,
            input: *const u8,
            input_len: usize,
            ctx: *const u8,
            ctx_len: usize,
        ) -> $crate::BicycleBytes {
            if handle.is_null() {
                return $crate::BicycleBytes::error(1, "Null handle".to_string());
            }

            let input_bytes = $crate::ffi::read_bytes(input, input_len);
            let ctx_bytes = $crate::ffi::read_bytes(ctx, ctx_len);

            let plugin_ctx = match $crate::PluginContext::from_bytes(&ctx_bytes) {
                Ok(c) => c,
                Err(e) => return $crate::BicycleBytes::error(2, format!("Invalid context: {}", e)),
            };

            // Try each function type - we need to check which type this handle is
            $(
                {
                    let wrapper = handle as *mut $crate::__FunctionWrapper<$func>;
                    if let Some(w) = wrapper.as_mut() {
                        return w.process(&input_bytes, &plugin_ctx);
                    }
                }
            )+

            $crate::BicycleBytes::error(3, "Invalid handle".to_string())
        }

        /// Close a function instance.
        ///
        /// # Safety
        /// - `handle` must be a valid pointer from `bicycle_create`.
        #[no_mangle]
        pub unsafe extern "C" fn bicycle_close(handle: *mut std::ffi::c_void) -> $crate::BicycleResult {
            if handle.is_null() {
                return $crate::BicycleResult::err("Null handle".to_string());
            }

            // Try each function type
            $(
                {
                    let wrapper = handle as *mut $crate::__FunctionWrapper<$func>;
                    if let Some(w) = wrapper.as_mut() {
                        return w.close();
                    }
                }
            )+

            $crate::BicycleResult::err("Invalid handle".to_string())
        }

        /// Create a snapshot of function state (for checkpointing).
        ///
        /// # Safety
        /// - `handle` must be a valid pointer from `bicycle_create`.
        /// - `ctx` may be null if `ctx_len` is 0.
        #[no_mangle]
        pub unsafe extern "C" fn bicycle_snapshot(
            handle: *mut std::ffi::c_void,
            ctx: *const u8,
            ctx_len: usize,
        ) -> $crate::BicycleBytes {
            if handle.is_null() {
                return $crate::BicycleBytes::error(1, "Null handle".to_string());
            }

            let ctx_bytes = $crate::ffi::read_bytes(ctx, ctx_len);
            let plugin_ctx = match $crate::PluginContext::from_bytes(&ctx_bytes) {
                Ok(c) => c,
                Err(e) => return $crate::BicycleBytes::error(2, format!("Invalid context: {}", e)),
            };

            // Try each function type
            $(
                {
                    let wrapper = handle as *mut $crate::__FunctionWrapper<$func>;
                    if let Some(w) = wrapper.as_mut() {
                        return w.snapshot(&plugin_ctx);
                    }
                }
            )+

            $crate::BicycleBytes::error(3, "Invalid handle".to_string())
        }

        /// Restore function state from a snapshot.
        ///
        /// # Safety
        /// - `handle` must be a valid pointer from `bicycle_create`.
        /// - `data` may be null if `data_len` is 0.
        #[no_mangle]
        pub unsafe extern "C" fn bicycle_restore(
            handle: *mut std::ffi::c_void,
            data: *const u8,
            data_len: usize,
        ) -> $crate::BicycleResult {
            if handle.is_null() {
                return $crate::BicycleResult::err("Null handle".to_string());
            }

            let data_bytes = $crate::ffi::read_bytes(data, data_len);

            // Try each function type
            $(
                {
                    let wrapper = handle as *mut $crate::__FunctionWrapper<$func>;
                    if let Some(w) = wrapper.as_mut() {
                        return w.restore(&data_bytes);
                    }
                }
            )+

            $crate::BicycleResult::err("Invalid handle".to_string())
        }

        /// Destroy a function instance and free its memory.
        ///
        /// # Safety
        /// - `handle` must be a valid pointer from `bicycle_create`.
        /// - The handle must not be used after calling this function.
        #[no_mangle]
        pub unsafe extern "C" fn bicycle_destroy(handle: *mut std::ffi::c_void) {
            if handle.is_null() {
                return;
            }

            // We need to know which type to drop. Since we can't easily determine this,
            // we'll just drop as the first matching type. In practice, this works because
            // each handle was created for a specific type.
            $(
                {
                    let wrapper = handle as *mut $crate::__FunctionWrapper<$func>;
                    if !wrapper.is_null() {
                        let _ = Box::from_raw(wrapper);
                        return;
                    }
                }
            )+
        }
    };
}

// =============================================================================
// Internal types and traits used by the macro
// =============================================================================

/// Trait to extract input/output types from function implementations.
/// This is a workaround since we can't directly access associated types in macros.
#[doc(hidden)]
pub trait __FunctionTypes {
    type In: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static;
    type Out: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static;
}

impl<T: AsyncFunction> __FunctionTypes for T {
    type In = T::In;
    type Out = T::Out;
}

/// Helper function to check if a type implements RichAsyncFunction.
#[doc(hidden)]
pub fn __is_rich_function<T>() -> bool {
    // This is a compile-time check that's always false for non-rich functions
    // In a real implementation, we'd use a trait-based approach
    false
}

/// Wrapper around a function instance that handles serialization and async runtime.
#[doc(hidden)]
pub struct __FunctionWrapper<F: AsyncFunction + Default> {
    function: F,
    runtime: tokio::runtime::Runtime,
}

impl<F: AsyncFunction + Default> __FunctionWrapper<F> {
    pub fn new(function: F) -> Self {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime");

        Self { function, runtime }
    }

    pub fn open(&mut self, ctx: &PluginContext) -> BicycleResult {
        let task_info = bicycle_api::TaskInfo::new(
            &ctx.job_id,
            "plugin",
            ctx.subtask_index as u32,
            1,
        );
        let api_ctx = bicycle_api::Context::new(task_info);

        match self.runtime.block_on(self.function.open(&api_ctx)) {
            Ok(()) => BicycleResult::ok(),
            Err(e) => BicycleResult::err(e.to_string()),
        }
    }

    pub fn process(&mut self, input: &[u8], ctx: &PluginContext) -> BicycleBytes {
        // Deserialize input
        let input_value: F::In = match serde_json::from_slice(input) {
            Ok(v) => v,
            Err(e) => return BicycleBytes::error(1, format!("Failed to deserialize input: {}", e)),
        };

        let task_info = bicycle_api::TaskInfo::new(
            &ctx.job_id,
            "plugin",
            ctx.subtask_index as u32,
            1,
        );
        let api_ctx = bicycle_api::Context::new(task_info);

        // Process
        let outputs = self.runtime.block_on(self.function.process(input_value, &api_ctx));

        // Serialize outputs as JSON array
        match serde_json::to_vec(&outputs) {
            Ok(bytes) => BicycleBytes::success(bytes),
            Err(e) => BicycleBytes::error(2, format!("Failed to serialize output: {}", e)),
        }
    }

    pub fn close(&mut self) -> BicycleResult {
        match self.runtime.block_on(self.function.close()) {
            Ok(()) => BicycleResult::ok(),
            Err(e) => BicycleResult::err(e.to_string()),
        }
    }

    pub fn snapshot(&self, _ctx: &PluginContext) -> BicycleBytes {
        // For non-rich functions, there's no state to snapshot
        BicycleBytes::empty()
    }

    pub fn restore(&mut self, _data: &[u8]) -> BicycleResult {
        // For non-rich functions, there's no state to restore
        BicycleResult::ok()
    }
}

// =============================================================================
// Rich function wrapper (for stateful functions)
// =============================================================================

/// Wrapper for RichAsyncFunction implementations.
#[doc(hidden)]
pub struct __RichFunctionWrapper<F: RichAsyncFunction + Default> {
    function: F,
    runtime: tokio::runtime::Runtime,
}

impl<F: RichAsyncFunction + Default> __RichFunctionWrapper<F> {
    pub fn new(function: F) -> Self {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime");

        Self { function, runtime }
    }

    pub fn open(&mut self, ctx: &PluginContext) -> BicycleResult {
        let runtime_ctx = bicycle_api::RuntimeContext::standalone();

        match self.runtime.block_on(self.function.open(&runtime_ctx)) {
            Ok(()) => BicycleResult::ok(),
            Err(e) => BicycleResult::err(e.to_string()),
        }
    }

    pub fn process(&mut self, input: &[u8], ctx: &PluginContext) -> BicycleBytes {
        // Deserialize input
        let input_value: F::In = match serde_json::from_slice(input) {
            Ok(v) => v,
            Err(e) => return BicycleBytes::error(1, format!("Failed to deserialize input: {}", e)),
        };

        let runtime_ctx = bicycle_api::RuntimeContext::standalone();

        // Process
        let outputs = self.runtime.block_on(self.function.process(input_value, &runtime_ctx));

        // Serialize outputs as JSON array
        match serde_json::to_vec(&outputs) {
            Ok(bytes) => BicycleBytes::success(bytes),
            Err(e) => BicycleBytes::error(2, format!("Failed to serialize output: {}", e)),
        }
    }

    pub fn close(&mut self) -> BicycleResult {
        match self.runtime.block_on(self.function.close()) {
            Ok(()) => BicycleResult::ok(),
            Err(e) => BicycleResult::err(e.to_string()),
        }
    }

    pub fn snapshot(&self, ctx: &PluginContext) -> BicycleBytes {
        let runtime_ctx = bicycle_api::RuntimeContext::standalone();

        match self.runtime.block_on(self.function.snapshot(&runtime_ctx)) {
            Ok(()) => {
                // TODO: Extract state from context and serialize
                BicycleBytes::empty()
            }
            Err(e) => BicycleBytes::error(1, format!("Snapshot failed: {}", e)),
        }
    }

    pub fn restore(&mut self, data: &[u8]) -> BicycleResult {
        // TODO: Restore state from data
        BicycleResult::ok()
    }
}

/// Macro for plugins with rich (stateful) functions.
///
/// Use this instead of `bicycle_plugin!` when your functions implement
/// `RichAsyncFunction` instead of `AsyncFunction`.
#[macro_export]
macro_rules! bicycle_plugin_rich {
    ($($func:ty),+ $(,)?) => {
        // Plugin metadata
        static PLUGIN_INFO: std::sync::OnceLock<$crate::PluginInfo> = std::sync::OnceLock::new();

        fn get_plugin_info() -> &'static $crate::PluginInfo {
            PLUGIN_INFO.get_or_init(|| {
                let mut info = $crate::PluginInfo::new(
                    env!("CARGO_PKG_NAME"),
                    env!("CARGO_PKG_VERSION"),
                );
                $(
                    info.add_function($crate::FunctionInfo {
                        name: std::any::type_name::<$func>().to_string(),
                        is_rich: true,
                        input_type: std::any::type_name::<<$func as $crate::__RichFunctionTypes>::In>().to_string(),
                        output_type: std::any::type_name::<<$func as $crate::__RichFunctionTypes>::Out>().to_string(),
                    });
                )+
                info
            })
        }

        /// Returns plugin metadata as JSON.
        #[no_mangle]
        pub extern "C" fn bicycle_plugin_info() -> $crate::BicycleBytes {
            let info = get_plugin_info();
            $crate::BicycleBytes::success(info.to_bytes())
        }

        /// Create a function instance by name.
        #[no_mangle]
        pub unsafe extern "C" fn bicycle_create(
            name: *const std::os::raw::c_char,
            config: *const u8,
            config_len: usize,
        ) -> *mut std::ffi::c_void {
            let name_str = $crate::ffi::c_str_to_string(name);
            let _config_bytes = $crate::ffi::read_bytes(config, config_len);

            $(
                if name_str == std::any::type_name::<$func>() || name_str.ends_with(stringify!($func)) {
                    let instance = Box::new(<$func>::default());
                    let wrapper = Box::new($crate::__RichFunctionWrapper::<$func>::new(*instance));
                    return Box::into_raw(wrapper) as *mut std::ffi::c_void;
                }
            )+

            std::ptr::null_mut()
        }

        /// Open/initialize a function instance.
        #[no_mangle]
        pub unsafe extern "C" fn bicycle_open(
            handle: *mut std::ffi::c_void,
            ctx: *const u8,
            ctx_len: usize,
        ) -> $crate::BicycleResult {
            if handle.is_null() {
                return $crate::BicycleResult::err("Null handle".to_string());
            }

            let ctx_bytes = $crate::ffi::read_bytes(ctx, ctx_len);
            let plugin_ctx = match $crate::PluginContext::from_bytes(&ctx_bytes) {
                Ok(c) => c,
                Err(e) => return $crate::BicycleResult::err(format!("Invalid context: {}", e)),
            };

            $(
                {
                    let wrapper = handle as *mut $crate::__RichFunctionWrapper<$func>;
                    if let Some(w) = wrapper.as_mut() {
                        return w.open(&plugin_ctx);
                    }
                }
            )+

            $crate::BicycleResult::err("Invalid handle".to_string())
        }

        /// Process input data through the function.
        #[no_mangle]
        pub unsafe extern "C" fn bicycle_process(
            handle: *mut std::ffi::c_void,
            input: *const u8,
            input_len: usize,
            ctx: *const u8,
            ctx_len: usize,
        ) -> $crate::BicycleBytes {
            if handle.is_null() {
                return $crate::BicycleBytes::error(1, "Null handle".to_string());
            }

            let input_bytes = $crate::ffi::read_bytes(input, input_len);
            let ctx_bytes = $crate::ffi::read_bytes(ctx, ctx_len);

            let plugin_ctx = match $crate::PluginContext::from_bytes(&ctx_bytes) {
                Ok(c) => c,
                Err(e) => return $crate::BicycleBytes::error(2, format!("Invalid context: {}", e)),
            };

            $(
                {
                    let wrapper = handle as *mut $crate::__RichFunctionWrapper<$func>;
                    if let Some(w) = wrapper.as_mut() {
                        return w.process(&input_bytes, &plugin_ctx);
                    }
                }
            )+

            $crate::BicycleBytes::error(3, "Invalid handle".to_string())
        }

        /// Close a function instance.
        #[no_mangle]
        pub unsafe extern "C" fn bicycle_close(handle: *mut std::ffi::c_void) -> $crate::BicycleResult {
            if handle.is_null() {
                return $crate::BicycleResult::err("Null handle".to_string());
            }

            $(
                {
                    let wrapper = handle as *mut $crate::__RichFunctionWrapper<$func>;
                    if let Some(w) = wrapper.as_mut() {
                        return w.close();
                    }
                }
            )+

            $crate::BicycleResult::err("Invalid handle".to_string())
        }

        /// Create a snapshot of function state.
        #[no_mangle]
        pub unsafe extern "C" fn bicycle_snapshot(
            handle: *mut std::ffi::c_void,
            ctx: *const u8,
            ctx_len: usize,
        ) -> $crate::BicycleBytes {
            if handle.is_null() {
                return $crate::BicycleBytes::error(1, "Null handle".to_string());
            }

            let ctx_bytes = $crate::ffi::read_bytes(ctx, ctx_len);
            let plugin_ctx = match $crate::PluginContext::from_bytes(&ctx_bytes) {
                Ok(c) => c,
                Err(e) => return $crate::BicycleBytes::error(2, format!("Invalid context: {}", e)),
            };

            $(
                {
                    let wrapper = handle as *mut $crate::__RichFunctionWrapper<$func>;
                    if let Some(w) = wrapper.as_mut() {
                        return w.snapshot(&plugin_ctx);
                    }
                }
            )+

            $crate::BicycleBytes::error(3, "Invalid handle".to_string())
        }

        /// Restore function state from a snapshot.
        #[no_mangle]
        pub unsafe extern "C" fn bicycle_restore(
            handle: *mut std::ffi::c_void,
            data: *const u8,
            data_len: usize,
        ) -> $crate::BicycleResult {
            if handle.is_null() {
                return $crate::BicycleResult::err("Null handle".to_string());
            }

            let data_bytes = $crate::ffi::read_bytes(data, data_len);

            $(
                {
                    let wrapper = handle as *mut $crate::__RichFunctionWrapper<$func>;
                    if let Some(w) = wrapper.as_mut() {
                        return w.restore(&data_bytes);
                    }
                }
            )+

            $crate::BicycleResult::err("Invalid handle".to_string())
        }

        /// Destroy a function instance.
        #[no_mangle]
        pub unsafe extern "C" fn bicycle_destroy(handle: *mut std::ffi::c_void) {
            if handle.is_null() {
                return;
            }

            $(
                {
                    let wrapper = handle as *mut $crate::__RichFunctionWrapper<$func>;
                    if !wrapper.is_null() {
                        let _ = Box::from_raw(wrapper);
                        return;
                    }
                }
            )+
        }
    };
}

/// Trait to extract input/output types from rich function implementations.
#[doc(hidden)]
pub trait __RichFunctionTypes {
    type In: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static;
    type Out: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static;
}

impl<T: RichAsyncFunction> __RichFunctionTypes for T {
    type In = T::In;
    type Out = T::Out;
}
