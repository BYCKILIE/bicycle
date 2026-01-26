//! Word count example plugin for Bicycle.
//!
//! This plugin demonstrates how to build native Bicycle streaming functions
//! that can be dynamically loaded by workers.
//!
//! The plugin exports two functions:
//! - `WordSplitter`: Splits input lines into words (AsyncFunction)
//! - `WordCounter`: Counts occurrences of each word (RichAsyncFunction - stateful)

use bicycle_api::prelude::*;
use bicycle_plugin::bicycle_plugin;
use std::collections::HashMap;

/// Splits input strings into lowercase words.
///
/// This is a stateless function that implements `AsyncFunction`.
#[derive(Default)]
pub struct WordSplitter;

#[async_trait]
impl AsyncFunction for WordSplitter {
    type In = String;
    type Out = String;

    async fn process(&mut self, input: String, _ctx: &Context) -> Vec<String> {
        input
            .split_whitespace()
            .map(|word| word.to_lowercase())
            .filter(|word| !word.is_empty())
            .collect()
    }

    fn name(&self) -> &str {
        "WordSplitter"
    }
}

/// Counts occurrences of words.
///
/// This is a stateful function that implements `RichAsyncFunction`.
/// It maintains a count for each word seen.
#[derive(Default)]
pub struct WordCounter {
    counts: HashMap<String, u64>,
}

#[async_trait]
impl RichAsyncFunction for WordCounter {
    type In = String;
    type Out = String;

    async fn open(&mut self, _ctx: &RuntimeContext) -> Result<()> {
        // Could restore state from checkpoint here
        Ok(())
    }

    async fn process(&mut self, word: String, _ctx: &RuntimeContext) -> Vec<String> {
        let count = self.counts.entry(word.clone()).or_insert(0);
        *count += 1;
        vec![format!("{}:{}", word, count)]
    }

    async fn snapshot(&self, _ctx: &RuntimeContext) -> Result<()> {
        // Could save state for checkpoint here
        Ok(())
    }

    fn name(&self) -> &str {
        "WordCounter"
    }
}

/// Converts strings to uppercase (simple example).
#[derive(Default)]
pub struct ToUppercase;

#[async_trait]
impl AsyncFunction for ToUppercase {
    type In = String;
    type Out = String;

    async fn process(&mut self, input: String, _ctx: &Context) -> Vec<String> {
        vec![input.to_uppercase()]
    }

    fn name(&self) -> &str {
        "ToUppercase"
    }
}

// Export all functions through the plugin macro
bicycle_plugin!(WordSplitter, ToUppercase);

// Note: For WordCounter, you would use bicycle_plugin_rich!(WordCounter);
// But for this example, we keep it simple with the non-rich functions.
