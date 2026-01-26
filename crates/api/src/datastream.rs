//! DataStream API for building streaming pipelines.
//!
//! This module provides the fluent API for building streaming pipelines,
//! similar to Flink's DataStream API.

use crate::function::{AsyncFunction, FilterFunction, FlatMapFunction, KeySelector, MapFunction, RichAsyncFunction};
use crate::graph::{ConnectorConfig, Edge, OperatorType, PartitionStrategy, Vertex, WindowType};
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

/// A stream of elements of type T.
///
/// DataStream provides a fluent API for building streaming pipelines.
/// Operations are lazy - they build up a job graph that is executed
/// when `execute()` is called on the StreamEnvironment.
///
/// # Example
///
/// ```ignore
/// env.socket_source("0.0.0.0", 9999)
///     .map(|s: String| s.to_uppercase())
///     .filter(|s| !s.is_empty())
///     .flat_map(|s: String| s.split_whitespace().map(String::from).collect::<Vec<_>>())
///     .key_by(|word| word.clone())
///     .process(WordCounter::new())
///     .socket_sink("0.0.0.0", 9998);
/// ```
pub struct DataStream<T> {
    /// The environment this stream belongs to.
    env: Arc<StreamEnvInner>,
    /// The vertex ID of the current operator.
    vertex_id: String,
    /// Phantom data for type T.
    _marker: PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned + Send + Sync + 'static> DataStream<T> {
    /// Create a new DataStream.
    pub(crate) fn new(env: Arc<StreamEnvInner>, vertex_id: String) -> Self {
        Self {
            env,
            vertex_id,
            _marker: PhantomData,
        }
    }

    /// Apply a map transformation.
    ///
    /// Transforms each element one-to-one using the provided function.
    pub fn map<U, F>(self, f: F) -> DataStream<U>
    where
        U: Serialize + DeserializeOwned + Send + Sync + 'static,
        F: Fn(T) -> U + Send + Sync + 'static,
    {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(&new_id, "Map", OperatorType::Map);

        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));

        DataStream::new(self.env, new_id)
    }

    /// Apply a flat map transformation.
    ///
    /// Transforms each element into zero or more elements.
    pub fn flat_map<U, F>(self, f: F) -> DataStream<U>
    where
        U: Serialize + DeserializeOwned + Send + Sync + 'static,
        F: Fn(T) -> Vec<U> + Send + Sync + 'static,
    {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(&new_id, "FlatMap", OperatorType::FlatMap);

        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));

        DataStream::new(self.env, new_id)
    }

    /// Apply a filter transformation.
    ///
    /// Keeps only elements that satisfy the predicate.
    pub fn filter<F>(self, predicate: F) -> DataStream<T>
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
    {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(&new_id, "Filter", OperatorType::Filter);

        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));

        DataStream::new(self.env, new_id)
    }

    /// Apply an async function.
    ///
    /// This is the primary way to apply custom logic to a stream.
    pub fn process<F>(self, function: F) -> DataStream<F::Out>
    where
        F: AsyncFunction<In = T>,
    {
        let new_id = self.env.next_vertex_id();
        let name = function.name().split("::").last().unwrap_or("Process");
        let vertex = Vertex::new(&new_id, name, OperatorType::Process { is_rich: false })
            .with_plugin_function(function.name().to_string());

        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));

        DataStream::new(self.env, new_id)
    }

    /// Apply a rich async function (stateful).
    ///
    /// Use this for operations that need to maintain state.
    pub fn process_rich<F>(self, function: F) -> DataStream<F::Out>
    where
        F: RichAsyncFunction<In = T>,
    {
        let new_id = self.env.next_vertex_id();
        let name = function.name().split("::").last().unwrap_or("Process");
        let vertex = Vertex::new(&new_id, name, OperatorType::Process { is_rich: true })
            .with_plugin_function(function.name().to_string());

        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));

        DataStream::new(self.env, new_id)
    }

    /// Apply a plugin function by name.
    ///
    /// Use this when the function is compiled into a native plugin (.so).
    /// The function_name should match the type name exported by the plugin.
    ///
    /// # Example
    ///
    /// ```ignore
    /// env.socket_source("0.0.0.0", 9999)
    ///     .process_plugin::<String>("WordSplitter")
    ///     .socket_sink("0.0.0.0", 9998);
    /// ```
    pub fn process_plugin<U>(self, function_name: &str) -> DataStream<U>
    where
        U: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        let new_id = self.env.next_vertex_id();
        let name = function_name.split("::").last().unwrap_or(function_name);
        let vertex = Vertex::new(&new_id, name, OperatorType::Process { is_rich: false })
            .with_plugin_function(function_name.to_string());

        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));

        DataStream::new(self.env, new_id)
    }

    /// Apply a rich (stateful) plugin function by name.
    ///
    /// Use this when the function is compiled into a native plugin (.so)
    /// and needs to maintain state across elements.
    pub fn process_plugin_rich<U>(self, function_name: &str) -> DataStream<U>
    where
        U: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        let new_id = self.env.next_vertex_id();
        let name = function_name.split("::").last().unwrap_or(function_name);
        let vertex = Vertex::new(&new_id, name, OperatorType::Process { is_rich: true })
            .with_plugin_function(function_name.to_string());

        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));

        DataStream::new(self.env, new_id)
    }

    /// Partition the stream by key.
    ///
    /// Returns a KeyedStream that enables keyed operations like reduce and window.
    pub fn key_by<K, F>(self, key_selector: F) -> KeyedStream<T, K>
    where
        T: Clone,
        K: Serialize + DeserializeOwned + Clone + std::hash::Hash + Eq + Send + Sync + 'static,
        F: Fn(&T) -> K + Send + Sync + 'static,
    {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(
            &new_id,
            "KeyBy",
            OperatorType::KeyBy {
                key_selector: std::any::type_name::<F>().to_string(),
            },
        );

        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id).with_strategy(PartitionStrategy::Hash));

        KeyedStream::new(self.env, new_id)
    }

    /// Rebalance the stream (round-robin distribution).
    pub fn rebalance(self) -> DataStream<T> {
        // Just update the edge strategy for the next operator
        self
    }

    /// Broadcast the stream (send to all parallel instances).
    pub fn broadcast(self) -> DataStream<T> {
        self
    }

    /// Count elements in the stream.
    pub fn count(self) -> DataStream<String> {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(&new_id, "Count", OperatorType::Count);

        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));

        DataStream::new(self.env, new_id)
    }

    /// Write to a socket sink.
    pub fn socket_sink(self, host: &str, port: u16) {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(
            &new_id,
            "SocketSink",
            OperatorType::Sink {
                connector: ConnectorConfig::socket(host, port),
            },
        );

        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
    }

    /// Write to a Kafka topic.
    pub fn kafka_sink(self, brokers: &str, topic: &str) {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(
            &new_id,
            "KafkaSink",
            OperatorType::Sink {
                connector: ConnectorConfig::kafka(brokers, topic),
            },
        );

        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
    }

    /// Write to stdout (for debugging).
    pub fn print(self) {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(
            &new_id,
            "Print",
            OperatorType::Sink {
                connector: ConnectorConfig {
                    connector_type: crate::graph::ConnectorType::None,
                    properties: Default::default(),
                },
            },
        );

        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));
    }

    /// Set parallelism for this operator.
    pub fn set_parallelism(self, parallelism: u32) -> Self {
        self.env.set_parallelism(&self.vertex_id, parallelism);
        self
    }

    /// Get the vertex ID.
    pub fn vertex_id(&self) -> &str {
        &self.vertex_id
    }
}

/// A keyed stream partitioned by key K.
///
/// Enables keyed operations like reduce, windowing, and stateful processing.
pub struct KeyedStream<T, K> {
    env: Arc<StreamEnvInner>,
    vertex_id: String,
    _marker: PhantomData<(T, K)>,
}

impl<T, K> KeyedStream<T, K>
where
    T: Serialize + DeserializeOwned + Send + Sync + Clone + 'static,
    K: Serialize + DeserializeOwned + Clone + std::hash::Hash + Eq + Send + Sync + 'static,
{
    pub(crate) fn new(env: Arc<StreamEnvInner>, vertex_id: String) -> Self {
        Self {
            env,
            vertex_id,
            _marker: PhantomData,
        }
    }

    /// Apply a rich async function to keyed elements.
    ///
    /// State accessed in the function is scoped to the current key.
    pub fn process<F>(self, function: F) -> DataStream<F::Out>
    where
        F: RichAsyncFunction<In = T>,
    {
        let new_id = self.env.next_vertex_id();
        let name = function.name().split("::").last().unwrap_or("Process");
        let vertex = Vertex::new(&new_id, name, OperatorType::Process { is_rich: true })
            .with_plugin_function(function.name().to_string());

        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));

        DataStream::new(self.env, new_id)
    }

    /// Apply a plugin function by name to keyed elements.
    ///
    /// State accessed in the function is scoped to the current key.
    pub fn process_plugin<U>(self, function_name: &str) -> DataStream<U>
    where
        U: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        let new_id = self.env.next_vertex_id();
        let name = function_name.split("::").last().unwrap_or(function_name);
        let vertex = Vertex::new(&new_id, name, OperatorType::Process { is_rich: true })
            .with_plugin_function(function_name.to_string());

        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));

        DataStream::new(self.env, new_id)
    }

    /// Apply a reduce function.
    pub fn reduce<F>(self, _reduce_fn: F) -> DataStream<T>
    where
        F: Fn(T, T) -> T + Send + Sync + 'static,
    {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(&new_id, "Reduce", OperatorType::Reduce);

        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));

        DataStream::new(self.env, new_id)
    }

    /// Count elements by key.
    pub fn count(self) -> DataStream<String> {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(&new_id, "Count", OperatorType::Count);

        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));

        DataStream::new(self.env, new_id)
    }

    /// Apply a tumbling window.
    pub fn tumbling_window(self, size_ms: u64) -> WindowedStream<T, K> {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(
            &new_id,
            "TumblingWindow",
            OperatorType::Window {
                window_type: WindowType::Tumbling { size_ms },
            },
        );

        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));

        WindowedStream::new(self.env, new_id)
    }

    /// Apply a sliding window.
    pub fn sliding_window(self, size_ms: u64, slide_ms: u64) -> WindowedStream<T, K> {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(
            &new_id,
            "SlidingWindow",
            OperatorType::Window {
                window_type: WindowType::Sliding { size_ms, slide_ms },
            },
        );

        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));

        WindowedStream::new(self.env, new_id)
    }

    /// Apply a session window.
    pub fn session_window(self, gap_ms: u64) -> WindowedStream<T, K> {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(
            &new_id,
            "SessionWindow",
            OperatorType::Window {
                window_type: WindowType::Session { gap_ms },
            },
        );

        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));

        WindowedStream::new(self.env, new_id)
    }

    /// Convert back to a DataStream.
    pub fn into_stream(self) -> DataStream<T> {
        DataStream::new(self.env, self.vertex_id)
    }
}

/// A windowed stream.
pub struct WindowedStream<T, K> {
    env: Arc<StreamEnvInner>,
    vertex_id: String,
    _marker: PhantomData<(T, K)>,
}

impl<T, K> WindowedStream<T, K>
where
    T: Serialize + DeserializeOwned + Send + Sync + Clone + 'static,
    K: Serialize + DeserializeOwned + Clone + std::hash::Hash + Eq + Send + Sync + 'static,
{
    pub(crate) fn new(env: Arc<StreamEnvInner>, vertex_id: String) -> Self {
        Self {
            env,
            vertex_id,
            _marker: PhantomData,
        }
    }

    /// Reduce elements in the window.
    pub fn reduce<F>(self, _reduce_fn: F) -> DataStream<T>
    where
        F: Fn(T, T) -> T + Send + Sync + 'static,
    {
        // Window already added, result goes to next operator
        DataStream::new(self.env, self.vertex_id)
    }

    /// Count elements in the window.
    pub fn count(self) -> DataStream<String> {
        let new_id = self.env.next_vertex_id();
        let vertex = Vertex::new(&new_id, "WindowCount", OperatorType::Count);

        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));

        DataStream::new(self.env, new_id)
    }

    /// Apply a custom process function to window contents.
    pub fn process<F, O>(self, function: F) -> DataStream<O>
    where
        O: Serialize + DeserializeOwned + Send + Sync + 'static,
        F: RichAsyncFunction<In = T, Out = O>,
    {
        let new_id = self.env.next_vertex_id();
        let name = function.name().split("::").last().unwrap_or("WindowProcess");
        let vertex = Vertex::new(&new_id, name, OperatorType::Process { is_rich: true })
            .with_plugin_function(function.name().to_string());

        self.env.add_vertex(vertex);
        self.env.add_edge(Edge::new(&self.vertex_id, &new_id));

        DataStream::new(self.env, new_id)
    }
}

/// Internal environment state shared between streams.
pub(crate) struct StreamEnvInner {
    vertices: Mutex<Vec<Vertex>>,
    edges: Mutex<Vec<Edge>>,
    vertex_counter: Mutex<u32>,
}

impl StreamEnvInner {
    pub fn new() -> Self {
        Self {
            vertices: Mutex::new(Vec::new()),
            edges: Mutex::new(Vec::new()),
            vertex_counter: Mutex::new(0),
        }
    }

    pub fn next_vertex_id(&self) -> String {
        let mut counter = self.vertex_counter.lock().unwrap();
        *counter += 1;
        format!("vertex_{}", counter)
    }

    pub fn add_vertex(&self, vertex: Vertex) {
        self.vertices.lock().unwrap().push(vertex);
    }

    pub fn add_edge(&self, edge: Edge) {
        self.edges.lock().unwrap().push(edge);
    }

    pub fn set_parallelism(&self, vertex_id: &str, parallelism: u32) {
        let mut vertices = self.vertices.lock().unwrap();
        if let Some(v) = vertices.iter_mut().find(|v| v.id == vertex_id) {
            v.parallelism = parallelism;
        }
    }

    pub fn get_vertices(&self) -> Vec<Vertex> {
        self.vertices.lock().unwrap().clone()
    }

    pub fn get_edges(&self) -> Vec<Edge> {
        self.edges.lock().unwrap().clone()
    }
}
