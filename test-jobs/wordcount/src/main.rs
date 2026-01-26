//! Word Count Job - A standalone streaming job for Bicycle.
//!
//! This job demonstrates how to create custom streaming logic using the
//! Bicycle API and deploy it as a Docker container.
//!
//! # Architecture
//!
//! Jobs are standalone executables that:
//! 1. Handle their own sourceAsyncFunction connections (e.g., TCP sockets)
//! 2. Process data using custom AsyncFunction/RichAsyncFunction traits
//! 3. Handle their own sink connections
//!
//! # Building
//!
//! ```bash
//! # Build the job
//! cargo build --release -p wordcount
//!
//! # Or build Docker image
//! docker build -f test-jobs/wordcount/Dockerfile -t bicycle-wordcount .
//! ```
//!
//! # Running
//!
//! ```bash
//! # Run directly
//! ./target/release/wordcount --source-port 9999 --sink-port 9998
//!
//! # Or run via Docker
//! docker run -p 9999:9999 -p 9998:9998 bicycle-wordcount
//!
//! # Or submit to cluster
//! bicycle submit --docker --image bicycle-wordcount
//! ```
//!
//! # Usage
//!
//! ```bash
//! # Send input (in one terminal)
//! nc localhost 9999
//! hello world
//! hello bicycle streaming
//!
//! # Receive output (in another terminal)
//! nc localhost 9998
//! # Output: hello:1, world:1, hello:2, bicycle:1, streaming:1
//! ```

use bicycle_api::prelude::*;
use clap::Parser;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, RwLock};

// ============================================================================
// Command Line Arguments
// ============================================================================

#[derive(Parser, Debug)]
#[command(name = "wordcount")]
#[command(about = "Word count streaming job - counts word occurrences")]
struct Args {
    /// Port to listen for input data
    #[arg(long, default_value = "9999")]
    source_port: u16,

    /// Port to send output data
    #[arg(long, default_value = "9998")]
    sink_port: u16,

    /// Host to bind to
    #[arg(long, default_value = "0.0.0.0")]
    host: String,
}

// ============================================================================
// Custom Functions using AsyncFunction / RichAsyncFunction traits
// ============================================================================

/// Splits input lines into individual words.
/// This is a stateless function (AsyncFunction).
pub struct WordSplitter;

#[async_trait]
impl AsyncFunction for WordSplitter {
    type In = String;
    type Out = String;

    async fn process(&mut self, input: String, _ctx: &Context) -> Vec<String> {
        input
            .split_whitespace()
            .map(|s| s.to_lowercase())
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect()
    }

    fn name(&self) -> &str {
        "WordSplitter"
    }
}

/// Counts occurrences of each word.
/// This is a stateful function (RichAsyncFunction).
pub struct WordCounter {
    counts: HashMap<String, u64>,
}

impl WordCounter {
    pub fn new() -> Self {
        Self {
            counts: HashMap::new(),
        }
    }
}

#[async_trait]
impl RichAsyncFunction for WordCounter {
    type In = String;
    type Out = String;

    async fn open(&mut self, ctx: &RuntimeContext) -> Result<()> {
        // Restore state from checkpoint if available
        if let Ok(Some(state)) = ctx.get_state::<HashMap<String, u64>>("counts") {
            self.counts = state;
            eprintln!("[WordCounter] Restored {} words from checkpoint", self.counts.len());
        }
        Ok(())
    }

    async fn process(&mut self, word: String, _ctx: &RuntimeContext) -> Vec<String> {
        let count = self.counts.entry(word.clone()).or_insert(0);
        *count += 1;
        vec![format!("{}:{}", word, count)]
    }

    async fn snapshot(&self, ctx: &RuntimeContext) -> Result<()> {
        ctx.save_state("counts", &self.counts)
    }

    fn name(&self) -> &str {
        "WordCounter"
    }
}

// ============================================================================
// Job Runtime
// ============================================================================

struct JobState {
    word_counter: RwLock<WordCounter>,
    output_tx: broadcast::Sender<String>,
}

async fn run_source(listener: TcpListener, state: Arc<JobState>) {
    eprintln!("[Source] Listening for input connections...");

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                eprintln!("[Source] Client connected: {}", addr);
                let state = state.clone();
                tokio::spawn(async move {
                    handle_input_connection(stream, state).await;
                    eprintln!("[Source] Client disconnected: {}", addr);
                });
            }
            Err(e) => {
                eprintln!("[Source] Accept error: {}", e);
            }
        }
    }
}

async fn handle_input_connection(stream: TcpStream, state: Arc<JobState>) {
    let reader = BufReader::new(stream);
    let mut lines = reader.lines();

    // Create instances of our functions
    let mut splitter = WordSplitter;
    let ctx = Context::standalone();

    while let Ok(Some(line)) = lines.next_line().await {
        if line.trim().is_empty() {
            continue;
        }

        // Step 1: Split line into words using AsyncFunction
        let words = splitter.process(line, &ctx).await;

        // Step 2: Count each word using RichAsyncFunction
        let mut counter = state.word_counter.write().await;
        let runtime_ctx = RuntimeContext::standalone();

        for word in words {
            let outputs = counter.process(word, &runtime_ctx).await;

            // Step 3: Send outputs to sink
            for output in outputs {
                let _ = state.output_tx.send(output);
            }
        }
    }
}

async fn run_sink(listener: TcpListener, mut output_rx: broadcast::Receiver<String>) {
    eprintln!("[Sink] Listening for output connections...");

    let clients: Arc<RwLock<Vec<tokio::sync::mpsc::Sender<String>>>> =
        Arc::new(RwLock::new(Vec::new()));

    // Spawn connection acceptor
    let clients_clone = clients.clone();
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    eprintln!("[Sink] Client connected: {}", addr);
                    let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(100);
                    clients_clone.write().await.push(tx);

                    tokio::spawn(async move {
                        let (_, mut writer) = stream.into_split();
                        while let Some(msg) = rx.recv().await {
                            let line = format!("{}\n", msg);
                            if writer.write_all(line.as_bytes()).await.is_err() {
                                break;
                            }
                        }
                        eprintln!("[Sink] Client disconnected: {}", addr);
                    });
                }
                Err(e) => {
                    eprintln!("[Sink] Accept error: {}", e);
                }
            }
        }
    });

    // Forward messages to all clients
    while let Ok(msg) = output_rx.recv().await {
        println!("{}", msg); // Also print to stdout
        let clients_guard = clients.read().await;
        for client in clients_guard.iter() {
            let _ = client.send(msg.clone()).await;
        }
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    eprintln!("╔════════════════════════════════════════╗");
    eprintln!("║     Bicycle Word Count Job             ║");
    eprintln!("╠════════════════════════════════════════╣");
    eprintln!("║  Input:  nc {} {:5}          ║", args.host, args.source_port);
    eprintln!("║  Output: nc {} {:5}          ║", args.host, args.sink_port);
    eprintln!("╠════════════════════════════════════════╣");
    eprintln!("║  Pipeline:                             ║");
    eprintln!("║    Socket -> WordSplitter ->           ║");
    eprintln!("║    WordCounter -> Socket               ║");
    eprintln!("╚════════════════════════════════════════╝");
    eprintln!();

    // Create broadcast channel for outputs
    let (output_tx, output_rx) = broadcast::channel::<String>(1024);

    // Create shared state
    let state = Arc::new(JobState {
        word_counter: RwLock::new(WordCounter::new()),
        output_tx,
    });

    // Bind sockets
    let source_listener = TcpListener::bind(format!("{}:{}", args.host, args.source_port)).await?;
    let sink_listener = TcpListener::bind(format!("{}:{}", args.host, args.sink_port)).await?;

    eprintln!("Waiting for connections...");
    eprintln!();

    // Run source and sink in parallel
    tokio::select! {
        _ = run_source(source_listener, state.clone()) => {}
        _ = run_sink(sink_listener, output_rx) => {}
    }

    Ok(())
}
