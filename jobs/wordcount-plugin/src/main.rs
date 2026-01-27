//! Word count job submission binary.
//!
//! This binary builds the job graph and outputs it as JSON to stdout.
//! The plugin (.so) is compiled separately and submitted with the graph.
//!
//! # Usage
//!
//! ```bash
//! # Build the plugin and binary
//! cargo build -p wordcount-plugin --release
//!
//! # Submit to cluster (reads graph JSON from stdin, plugin from --plugin)
//! ./target/release/wordcount-plugin | bicycle submit --plugin target/release/libwordcount_plugin.so
//!
//! # Or manually specify the job:
//! bicycle submit ./target/release/wordcount-plugin --plugin ./target/release/libwordcount_plugin.so
//! ```

use bicycle_api::prelude::*;

fn main() {
    // Create the streaming environment with configuration
    let env = StreamEnvironment::builder()
        .parallelism(2)              // Default parallelism for all operators
        .max_parallelism(16)         // Max parallelism for rescaling
        .checkpoint_interval(30_000) // Checkpoint every 30 seconds
        .build();

    // Build the word count pipeline with UIDs and names for state recovery
    env.socket_source("0.0.0.0", 9999)
        .uid("source-v1")                    // Stable UID for state recovery
        .name("Socket Input")                // Display name in UI
        .process_plugin::<String>("WordSplitter")
        .uid("splitter-v1")
        .name("Word Splitter")
        .set_parallelism(4)                  // Override parallelism for this operator
        .set_max_parallelism(16)
        .slot_sharing_group("processing")    // Separate slot group for heavy processing
        .socket_sink("0.0.0.0", 9998)
        .uid("sink-v1")
        .name("Socket Output");

    // Build and optimize the job graph
    let (graph, optimized) = env.execute_optimized("wordcount-plugin")
        .expect("Failed to build job graph");

    // Print optimization summary to stderr (so it doesn't interfere with JSON output)
    eprintln!();
    optimized.print_summary();
    eprintln!();

    // Output the original graph as JSON (to stdout for the CLI to capture)
    let json = serde_json::to_string_pretty(&graph).expect("Failed to serialize graph");
    println!("{}", json);
}
