//! Function traits for user-defined operators.
//!
//! This module defines the core traits that users implement to create
//! custom streaming operators, similar to Flink's AsyncFunction and
//! RichAsyncFunction.

use crate::context::{Context, RuntimeContext};
use crate::Result;
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};

/// A stateless async function for processing stream elements.
///
/// This is the simplest function type, suitable for operations that don't
/// need to maintain state across elements or access runtime context.
///
/// # Example
///
/// ```ignore
/// use bicycle_api::prelude::*;
///
/// struct ToUppercase;
///
/// #[async_trait]
/// impl AsyncFunction for ToUppercase {
///     type In = String;
///     type Out = String;
///
///     async fn process(&mut self, input: String, _ctx: &Context) -> Vec<String> {
///         vec![input.to_uppercase()]
///     }
/// }
/// ```
#[async_trait]
pub trait AsyncFunction: Send + Sync + 'static {
    /// Input type.
    type In: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Output type.
    type Out: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Process a single input element and produce zero or more outputs.
    ///
    /// This method is called for each element in the input stream.
    async fn process(&mut self, input: Self::In, ctx: &Context) -> Vec<Self::Out>;

    /// Called once when the operator starts.
    ///
    /// Override this to perform initialization logic.
    async fn open(&mut self, _ctx: &Context) -> Result<()> {
        Ok(())
    }

    /// Called once when the operator closes.
    ///
    /// Override this to perform cleanup logic.
    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    /// Returns the name of this function for debugging/logging.
    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }
}

/// A stateful async function with access to keyed state and runtime context.
///
/// This is the more powerful function type, suitable for operations that need
/// to maintain state (like aggregations, joins, or sessions) or access
/// runtime information.
///
/// State is automatically checkpointed and restored on failure recovery.
///
/// # Example
///
/// ```ignore
/// use bicycle_api::prelude::*;
/// use std::collections::HashMap;
///
/// struct WordCounter {
///     counts: HashMap<String, u64>,
/// }
///
/// #[async_trait]
/// impl RichAsyncFunction for WordCounter {
///     type In = String;
///     type Out = String;
///
///     async fn open(&mut self, ctx: &RuntimeContext) -> Result<()> {
///         // Restore state from checkpoint if available
///         if let Some(state) = ctx.get_state("counts")? {
///             self.counts = state;
///         }
///         Ok(())
///     }
///
///     async fn process(&mut self, word: String, ctx: &RuntimeContext) -> Vec<String> {
///         let count = self.counts.entry(word.clone()).or_insert(0);
///         *count += 1;
///         vec![format!("{}:{}", word, count)]
///     }
///
///     async fn snapshot(&self, ctx: &RuntimeContext) -> Result<()> {
///         ctx.save_state("counts", &self.counts)
///     }
/// }
/// ```
#[async_trait]
pub trait RichAsyncFunction: Send + Sync + 'static {
    /// Input type.
    type In: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Output type.
    type Out: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Process a single input element with full runtime context.
    ///
    /// This method is called for each element in the input stream.
    /// Use the RuntimeContext to access keyed state.
    async fn process(&mut self, input: Self::In, ctx: &RuntimeContext) -> Vec<Self::Out>;

    /// Called once when the operator starts.
    ///
    /// This is where you should restore state from a checkpoint if available.
    async fn open(&mut self, _ctx: &RuntimeContext) -> Result<()> {
        Ok(())
    }

    /// Called once when the operator closes.
    ///
    /// Override this to perform cleanup logic.
    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    /// Called on checkpoint to save state.
    ///
    /// This method is called during checkpoint barriers. Use the RuntimeContext
    /// to persist any state that should survive failures.
    async fn snapshot(&self, _ctx: &RuntimeContext) -> Result<()> {
        Ok(())
    }

    /// Returns the name of this function for debugging/logging.
    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }
}

/// A simple map function that transforms each element one-to-one.
#[async_trait]
pub trait MapFunction: Send + Sync + 'static {
    type In: Serialize + DeserializeOwned + Send + Sync + 'static;
    type Out: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Transform a single element.
    async fn map(&mut self, input: Self::In) -> Self::Out;
}

/// A flat map function that transforms each element into zero or more elements.
#[async_trait]
pub trait FlatMapFunction: Send + Sync + 'static {
    type In: Serialize + DeserializeOwned + Send + Sync + 'static;
    type Out: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Transform a single element into zero or more elements.
    async fn flat_map(&mut self, input: Self::In) -> Vec<Self::Out>;
}

/// A filter function that decides whether to keep elements.
#[async_trait]
pub trait FilterFunction: Send + Sync + 'static {
    type T: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Return true to keep the element, false to filter it out.
    async fn filter(&mut self, input: &Self::T) -> bool;
}

/// A key selector function for partitioning streams.
pub trait KeySelector: Send + Sync + 'static {
    type T: Send + Sync + 'static;
    type Key: Serialize + DeserializeOwned + Send + Sync + Clone + std::hash::Hash + Eq + 'static;

    /// Extract a key from an element.
    fn get_key(&self, input: &Self::T) -> Self::Key;
}

/// A reduce function for combining elements.
#[async_trait]
pub trait ReduceFunction: Send + Sync + 'static {
    type T: Serialize + DeserializeOwned + Send + Sync + Clone + 'static;

    /// Combine two elements into one.
    async fn reduce(&mut self, a: Self::T, b: Self::T) -> Self::T;
}

/// An aggregate function for computing aggregates over windows.
#[async_trait]
pub trait AggregateFunction: Send + Sync + 'static {
    /// Input type.
    type In: Serialize + DeserializeOwned + Send + Sync + 'static;
    /// Accumulator type.
    type Acc: Serialize + DeserializeOwned + Send + Sync + Default + Clone + 'static;
    /// Output type.
    type Out: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Create the initial accumulator value.
    fn create_accumulator(&self) -> Self::Acc {
        Self::Acc::default()
    }

    /// Add an element to the accumulator.
    async fn add(&mut self, acc: &mut Self::Acc, input: Self::In);

    /// Get the result from the accumulator.
    async fn get_result(&self, acc: &Self::Acc) -> Self::Out;

    /// Merge two accumulators (for session window merging).
    async fn merge(&mut self, a: Self::Acc, b: Self::Acc) -> Self::Acc;
}

// Blanket implementation: MapFunction -> AsyncFunction
#[async_trait]
impl<F: MapFunction> AsyncFunction for MapFunctionAdapter<F> {
    type In = F::In;
    type Out = F::Out;

    async fn process(&mut self, input: Self::In, _ctx: &Context) -> Vec<Self::Out> {
        vec![self.0.map(input).await]
    }
}

/// Adapter to use MapFunction as AsyncFunction.
pub struct MapFunctionAdapter<F>(pub F);

// Blanket implementation: FlatMapFunction -> AsyncFunction
#[async_trait]
impl<F: FlatMapFunction> AsyncFunction for FlatMapFunctionAdapter<F> {
    type In = F::In;
    type Out = F::Out;

    async fn process(&mut self, input: Self::In, _ctx: &Context) -> Vec<Self::Out> {
        self.0.flat_map(input).await
    }
}

/// Adapter to use FlatMapFunction as AsyncFunction.
pub struct FlatMapFunctionAdapter<F>(pub F);

// Blanket implementation: FilterFunction -> AsyncFunction
#[async_trait]
impl<F: FilterFunction> AsyncFunction for FilterFunctionAdapter<F> {
    type In = F::T;
    type Out = F::T;

    async fn process(&mut self, input: Self::In, _ctx: &Context) -> Vec<Self::Out> {
        if self.0.filter(&input).await {
            vec![input]
        } else {
            vec![]
        }
    }
}

/// Adapter to use FilterFunction as AsyncFunction.
pub struct FilterFunctionAdapter<F>(pub F);

/// Wrapper for closure-based key selectors.
pub struct FnKeySelector<T, K, F>
where
    F: Fn(&T) -> K + Send + Sync + 'static,
{
    f: F,
    _marker: std::marker::PhantomData<(T, K)>,
}

impl<T, K, F> FnKeySelector<T, K, F>
where
    F: Fn(&T) -> K + Send + Sync + 'static,
{
    pub fn new(f: F) -> Self {
        Self {
            f,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T, K, F> KeySelector for FnKeySelector<T, K, F>
where
    T: Send + Sync + 'static,
    K: Serialize + DeserializeOwned + Send + Sync + Clone + std::hash::Hash + Eq + 'static,
    F: Fn(&T) -> K + Send + Sync + 'static,
{
    type T = T;
    type Key = K;

    fn get_key(&self, input: &T) -> K {
        (self.f)(input)
    }
}
