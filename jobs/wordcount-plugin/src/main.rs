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
    // Create the streaming environment
    let env = StreamEnvironment::new();

    // Build the word count pipeline
    // Source: Read lines from socket on port 9999
    // Process: Split into words using the WordSplitter plugin function
    // Sink: Write results to socket on port 9998
    env.socket_source("0.0.0.0", 9999)
        .process_plugin::<String>("WordSplitter")  // Uses the plugin function
        .socket_sink("0.0.0.0", 9998);

    // Build and serialize the job graph
    let graph = env.execute("wordcount-plugin").expect("Failed to build job graph");

    // Output as JSON (to stdout for the CLI to capture)
    let json = serde_json::to_string_pretty(&graph).expect("Failed to serialize graph");
    println!("{}", json);
}
