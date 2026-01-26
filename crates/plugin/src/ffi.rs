//! FFI types for the Bicycle plugin ABI.
//!
//! These types use `#[repr(C)]` to ensure a stable ABI across the FFI boundary.
//! Since Rust doesn't have a stable ABI, all plugin functions use `extern "C"`.

use std::ffi::CStr;
use std::os::raw::c_char;
use std::ptr;

/// FFI-safe bytes with optional error information.
///
/// Used for returning data from plugin functions. If `error_code` is non-zero,
/// the `error_msg_ptr` and `error_msg_len` contain the error message.
#[repr(C)]
pub struct BicycleBytes {
    /// Pointer to the data bytes (allocated by plugin).
    pub ptr: *mut u8,
    /// Length of the data in bytes.
    pub len: usize,
    /// Error code (0 = success, non-zero = error).
    pub error_code: i32,
    /// Pointer to error message (if error_code != 0).
    pub error_msg_ptr: *mut u8,
    /// Length of error message.
    pub error_msg_len: usize,
}

impl BicycleBytes {
    /// Create a successful result with data.
    pub fn success(data: Vec<u8>) -> Self {
        let len = data.len();
        let ptr = if len > 0 {
            let boxed = data.into_boxed_slice();
            let ptr = Box::into_raw(boxed) as *mut u8;
            ptr
        } else {
            ptr::null_mut()
        };

        Self {
            ptr,
            len,
            error_code: 0,
            error_msg_ptr: ptr::null_mut(),
            error_msg_len: 0,
        }
    }

    /// Create an empty successful result.
    pub fn empty() -> Self {
        Self {
            ptr: ptr::null_mut(),
            len: 0,
            error_code: 0,
            error_msg_ptr: ptr::null_mut(),
            error_msg_len: 0,
        }
    }

    /// Create an error result.
    pub fn error(code: i32, message: String) -> Self {
        let msg_bytes = message.into_bytes();
        let msg_len = msg_bytes.len();
        let msg_ptr = if msg_len > 0 {
            let boxed = msg_bytes.into_boxed_slice();
            Box::into_raw(boxed) as *mut u8
        } else {
            ptr::null_mut()
        };

        Self {
            ptr: ptr::null_mut(),
            len: 0,
            error_code: code,
            error_msg_ptr: msg_ptr,
            error_msg_len: msg_len,
        }
    }

    /// Check if this result is an error.
    pub fn is_error(&self) -> bool {
        self.error_code != 0
    }

    /// Convert to a Rust Vec<u8> if successful.
    ///
    /// # Safety
    /// This consumes the data. The caller must not use the pointer after calling this.
    pub unsafe fn into_vec(self) -> Result<Vec<u8>, String> {
        if self.is_error() {
            let error_msg = if !self.error_msg_ptr.is_null() && self.error_msg_len > 0 {
                let slice = std::slice::from_raw_parts(self.error_msg_ptr, self.error_msg_len);
                String::from_utf8_lossy(slice).to_string()
            } else {
                format!("Error code: {}", self.error_code)
            };
            Err(error_msg)
        } else if self.ptr.is_null() || self.len == 0 {
            Ok(Vec::new())
        } else {
            let slice = std::slice::from_raw_parts(self.ptr, self.len);
            Ok(slice.to_vec())
        }
    }
}

impl Default for BicycleBytes {
    fn default() -> Self {
        Self::empty()
    }
}

/// FFI-safe result type for operations that don't return data.
#[repr(C)]
pub struct BicycleResult {
    /// Whether the operation succeeded.
    pub success: bool,
    /// Pointer to error message (if !success).
    pub error_msg_ptr: *mut u8,
    /// Length of error message.
    pub error_msg_len: usize,
}

impl BicycleResult {
    /// Create a successful result.
    pub fn ok() -> Self {
        Self {
            success: true,
            error_msg_ptr: ptr::null_mut(),
            error_msg_len: 0,
        }
    }

    /// Create an error result.
    pub fn err(message: String) -> Self {
        let msg_bytes = message.into_bytes();
        let msg_len = msg_bytes.len();
        let msg_ptr = if msg_len > 0 {
            let boxed = msg_bytes.into_boxed_slice();
            Box::into_raw(boxed) as *mut u8
        } else {
            ptr::null_mut()
        };

        Self {
            success: false,
            error_msg_ptr: msg_ptr,
            error_msg_len: msg_len,
        }
    }

    /// Get the error message if this is an error.
    ///
    /// # Safety
    /// The error message pointer must be valid if success is false.
    pub unsafe fn error_message(&self) -> Option<String> {
        if self.success {
            None
        } else if !self.error_msg_ptr.is_null() && self.error_msg_len > 0 {
            let slice = std::slice::from_raw_parts(self.error_msg_ptr, self.error_msg_len);
            Some(String::from_utf8_lossy(slice).to_string())
        } else {
            Some("Unknown error".to_string())
        }
    }
}

impl Default for BicycleResult {
    fn default() -> Self {
        Self::ok()
    }
}

/// Plugin metadata returned by `bicycle_plugin_info`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PluginInfo {
    /// Plugin ABI version (for compatibility checking).
    pub abi_version: u32,
    /// Plugin name.
    pub name: String,
    /// Plugin version.
    pub version: String,
    /// List of function names exported by this plugin.
    pub functions: Vec<FunctionInfo>,
}

/// Information about a single function in the plugin.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FunctionInfo {
    /// Function name (used to create/invoke).
    pub name: String,
    /// Whether this is a rich (stateful) function.
    pub is_rich: bool,
    /// Input type name (for documentation).
    pub input_type: String,
    /// Output type name (for documentation).
    pub output_type: String,
}

impl PluginInfo {
    /// Current ABI version.
    pub const ABI_VERSION: u32 = 1;

    /// Create new plugin info.
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            abi_version: Self::ABI_VERSION,
            name: name.into(),
            version: version.into(),
            functions: Vec::new(),
        }
    }

    /// Add a function to the plugin info.
    pub fn add_function(&mut self, info: FunctionInfo) {
        self.functions.push(info);
    }

    /// Serialize to JSON bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    /// Deserialize from JSON bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, String> {
        serde_json::from_slice(data).map_err(|e| e.to_string())
    }
}

/// Context passed to plugin functions during processing.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PluginContext {
    /// Job ID.
    pub job_id: String,
    /// Task ID.
    pub task_id: String,
    /// Subtask index.
    pub subtask_index: i32,
    /// Current timestamp (milliseconds since epoch).
    pub timestamp: i64,
    /// Current key (if keyed stream, JSON-encoded).
    pub current_key: Option<String>,
}

impl Default for PluginContext {
    fn default() -> Self {
        Self {
            job_id: String::new(),
            task_id: String::new(),
            subtask_index: 0,
            timestamp: 0,
            current_key: None,
        }
    }
}

impl PluginContext {
    /// Serialize to JSON bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    /// Deserialize from JSON bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, String> {
        serde_json::from_slice(data).map_err(|e| e.to_string())
    }
}

// =============================================================================
// Memory management exports (used by host to free plugin-allocated memory)
// =============================================================================

/// Allocate memory in the plugin's address space.
///
/// # Safety
/// Returns a pointer that must be freed with `bicycle_free`.
#[no_mangle]
pub unsafe extern "C" fn bicycle_alloc(len: usize) -> *mut u8 {
    if len == 0 {
        return ptr::null_mut();
    }
    let layout = std::alloc::Layout::from_size_align(len, 1).unwrap();
    std::alloc::alloc(layout)
}

/// Free memory allocated by the plugin.
///
/// # Safety
/// `ptr` must have been allocated by `bicycle_alloc` with the same `len`.
#[no_mangle]
pub unsafe extern "C" fn bicycle_free(ptr: *mut u8, len: usize) {
    if !ptr.is_null() && len > 0 {
        let layout = std::alloc::Layout::from_size_align(len, 1).unwrap();
        std::alloc::dealloc(ptr, layout);
    }
}

/// Free a BicycleBytes structure and its contents.
///
/// # Safety
/// The BicycleBytes must have been created by plugin functions.
#[no_mangle]
pub unsafe extern "C" fn bicycle_free_bytes(bytes: BicycleBytes) {
    if !bytes.ptr.is_null() && bytes.len > 0 {
        let _ = Box::from_raw(std::slice::from_raw_parts_mut(bytes.ptr, bytes.len));
    }
    if !bytes.error_msg_ptr.is_null() && bytes.error_msg_len > 0 {
        let _ = Box::from_raw(std::slice::from_raw_parts_mut(
            bytes.error_msg_ptr,
            bytes.error_msg_len,
        ));
    }
}

/// Free a BicycleResult structure and its contents.
///
/// # Safety
/// The BicycleResult must have been created by plugin functions.
#[no_mangle]
pub unsafe extern "C" fn bicycle_free_result(result: BicycleResult) {
    if !result.error_msg_ptr.is_null() && result.error_msg_len > 0 {
        let _ = Box::from_raw(std::slice::from_raw_parts_mut(
            result.error_msg_ptr,
            result.error_msg_len,
        ));
    }
}

/// Helper to convert a C string to a Rust string.
///
/// # Safety
/// The pointer must be a valid null-terminated C string.
pub unsafe fn c_str_to_string(ptr: *const c_char) -> String {
    if ptr.is_null() {
        String::new()
    } else {
        CStr::from_ptr(ptr).to_string_lossy().to_string()
    }
}

/// Helper to read bytes from FFI pointers.
///
/// # Safety
/// The pointer and length must be valid.
pub unsafe fn read_bytes(ptr: *const u8, len: usize) -> Vec<u8> {
    if ptr.is_null() || len == 0 {
        Vec::new()
    } else {
        std::slice::from_raw_parts(ptr, len).to_vec()
    }
}
