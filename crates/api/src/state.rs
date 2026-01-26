//! State types for stateful stream processing.
//!
//! This module provides state abstractions similar to Flink's state API:
//! - ValueState: Single value per key
//! - ListState: List of values per key
//! - MapState: Map of key-value pairs per key

use crate::context::StateBackend;
use crate::{Error, Result};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

/// Value state - stores a single value per key.
///
/// # Example
///
/// ```ignore
/// let state = ctx.get_value_state::<i64>("counter");
/// let current = state.value().unwrap_or(0);
/// state.update(current + 1)?;
/// ```
pub struct ValueState<T> {
    name: String,
    backend: Arc<dyn StateBackend>,
    key: Option<Vec<u8>>,
    _marker: PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned + Default + Clone + Send + Sync + 'static> ValueState<T> {
    pub(crate) fn new(name: String, backend: Arc<dyn StateBackend>, key: Option<Vec<u8>>) -> Self {
        Self {
            name,
            backend,
            key,
            _marker: PhantomData,
        }
    }

    /// Get the current value.
    pub fn value(&self) -> Result<Option<T>> {
        let key = self.state_key();
        match self.backend.get_bytes(&key)? {
            Some(data) => {
                let value: T = serde_json::from_slice(&data)
                    .map_err(|e| Error::Serialization(e.to_string()))?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Update the value.
    pub fn update(&self, value: T) -> Result<()> {
        let key = self.state_key();
        let data = serde_json::to_vec(&value)
            .map_err(|e| Error::Serialization(e.to_string()))?;
        self.backend.put_bytes(&key, data)
    }

    /// Clear the value.
    pub fn clear(&self) -> Result<()> {
        let key = self.state_key();
        self.backend.delete(&key)
    }

    fn state_key(&self) -> String {
        match &self.key {
            Some(k) => format!("{}/{}", hex_encode(k), self.name),
            None => self.name.clone(),
        }
    }
}

/// List state - stores a list of values per key.
///
/// # Example
///
/// ```ignore
/// let state = ctx.get_list_state::<String>("buffer");
/// state.add("element".to_string())?;
/// let all = state.get()?;
/// ```
pub struct ListState<T> {
    name: String,
    backend: Arc<dyn StateBackend>,
    key: Option<Vec<u8>>,
    _marker: PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static> ListState<T> {
    pub(crate) fn new(name: String, backend: Arc<dyn StateBackend>, key: Option<Vec<u8>>) -> Self {
        Self {
            name,
            backend,
            key,
            _marker: PhantomData,
        }
    }

    /// Get all values in the list.
    pub fn get(&self) -> Result<Vec<T>> {
        let key = self.state_key();
        match self.backend.get_bytes(&key)? {
            Some(data) => {
                let list: Vec<T> = serde_json::from_slice(&data)
                    .map_err(|e| Error::Serialization(e.to_string()))?;
                Ok(list)
            }
            None => Ok(Vec::new()),
        }
    }

    /// Add a value to the list.
    pub fn add(&self, value: T) -> Result<()> {
        let key = self.state_key();
        let mut list = self.get()?;
        list.push(value);
        let data = serde_json::to_vec(&list)
            .map_err(|e| Error::Serialization(e.to_string()))?;
        self.backend.put_bytes(&key, data)
    }

    /// Add multiple values to the list.
    pub fn add_all(&self, values: Vec<T>) -> Result<()> {
        let key = self.state_key();
        let mut list = self.get()?;
        list.extend(values);
        let data = serde_json::to_vec(&list)
            .map_err(|e| Error::Serialization(e.to_string()))?;
        self.backend.put_bytes(&key, data)
    }

    /// Update the entire list.
    pub fn update(&self, values: Vec<T>) -> Result<()> {
        let key = self.state_key();
        let data = serde_json::to_vec(&values)
            .map_err(|e| Error::Serialization(e.to_string()))?;
        self.backend.put_bytes(&key, data)
    }

    /// Clear the list.
    pub fn clear(&self) -> Result<()> {
        let key = self.state_key();
        self.backend.delete(&key)
    }

    /// Check if the list is empty.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.get()?.is_empty())
    }

    fn state_key(&self) -> String {
        match &self.key {
            Some(k) => format!("{}/{}", hex_encode(k), self.name),
            None => self.name.clone(),
        }
    }
}

/// Map state - stores a map of key-value pairs per key.
///
/// # Example
///
/// ```ignore
/// let state = ctx.get_map_state::<String, i64>("counts");
/// state.put("word".to_string(), 42)?;
/// let count = state.get(&"word".to_string())?;
/// ```
pub struct MapState<K, V> {
    name: String,
    backend: Arc<dyn StateBackend>,
    key: Option<Vec<u8>>,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> MapState<K, V>
where
    K: Serialize + DeserializeOwned + Clone + std::hash::Hash + Eq + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    pub(crate) fn new(name: String, backend: Arc<dyn StateBackend>, key: Option<Vec<u8>>) -> Self {
        Self {
            name,
            backend,
            key,
            _marker: PhantomData,
        }
    }

    /// Get a value by key.
    pub fn get(&self, map_key: &K) -> Result<Option<V>> {
        let map = self.get_map()?;
        let key_json = serde_json::to_string(map_key)
            .map_err(|e| Error::Serialization(e.to_string()))?;
        Ok(map.get(&key_json).cloned())
    }

    /// Put a key-value pair.
    pub fn put(&self, map_key: K, value: V) -> Result<()> {
        let mut map = self.get_map()?;
        let key_json = serde_json::to_string(&map_key)
            .map_err(|e| Error::Serialization(e.to_string()))?;
        map.insert(key_json, value);
        self.put_map(&map)
    }

    /// Remove a key.
    pub fn remove(&self, map_key: &K) -> Result<Option<V>> {
        let mut map = self.get_map()?;
        let key_json = serde_json::to_string(map_key)
            .map_err(|e| Error::Serialization(e.to_string()))?;
        let old = map.remove(&key_json);
        self.put_map(&map)?;
        Ok(old)
    }

    /// Check if a key exists.
    pub fn contains(&self, map_key: &K) -> Result<bool> {
        let map = self.get_map()?;
        let key_json = serde_json::to_string(map_key)
            .map_err(|e| Error::Serialization(e.to_string()))?;
        Ok(map.contains_key(&key_json))
    }

    /// Get all entries.
    pub fn entries(&self) -> Result<Vec<(K, V)>> {
        let map = self.get_map()?;
        let mut result = Vec::new();
        for (k, v) in map {
            let key: K = serde_json::from_str(&k)
                .map_err(|e| Error::Serialization(e.to_string()))?;
            result.push((key, v));
        }
        Ok(result)
    }

    /// Get all keys.
    pub fn keys(&self) -> Result<Vec<K>> {
        let map = self.get_map()?;
        let mut result = Vec::new();
        for k in map.keys() {
            let key: K = serde_json::from_str(k)
                .map_err(|e| Error::Serialization(e.to_string()))?;
            result.push(key);
        }
        Ok(result)
    }

    /// Get all values.
    pub fn values(&self) -> Result<Vec<V>> {
        let map = self.get_map()?;
        Ok(map.into_values().collect())
    }

    /// Clear the map.
    pub fn clear(&self) -> Result<()> {
        let key = self.state_key();
        self.backend.delete(&key)
    }

    /// Check if the map is empty.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.get_map()?.is_empty())
    }

    fn get_map(&self) -> Result<HashMap<String, V>> {
        let key = self.state_key();
        match self.backend.get_bytes(&key)? {
            Some(data) => {
                let map: HashMap<String, V> = serde_json::from_slice(&data)
                    .map_err(|e| Error::Serialization(e.to_string()))?;
                Ok(map)
            }
            None => Ok(HashMap::new()),
        }
    }

    fn put_map(&self, map: &HashMap<String, V>) -> Result<()> {
        let key = self.state_key();
        let data = serde_json::to_vec(map)
            .map_err(|e| Error::Serialization(e.to_string()))?;
        self.backend.put_bytes(&key, data)
    }

    fn state_key(&self) -> String {
        match &self.key {
            Some(k) => format!("{}/{}", hex_encode(k), self.name),
            None => self.name.clone(),
        }
    }
}

fn hex_encode(data: &[u8]) -> String {
    data.iter().map(|b| format!("{:02x}", b)).collect()
}

/// State descriptor - describes a state to be created.
#[derive(Debug, Clone)]
pub enum StateDescriptor<T> {
    Value { name: String, default: Option<T> },
    List { name: String },
    Map { name: String },
}

impl<T: Default> StateDescriptor<T> {
    /// Create a value state descriptor.
    pub fn value(name: impl Into<String>) -> Self {
        StateDescriptor::Value {
            name: name.into(),
            default: Some(T::default()),
        }
    }

    /// Create a list state descriptor.
    pub fn list(name: impl Into<String>) -> Self {
        StateDescriptor::List { name: name.into() }
    }

    /// Create a map state descriptor.
    pub fn map(name: impl Into<String>) -> Self {
        StateDescriptor::Map { name: name.into() }
    }
}
