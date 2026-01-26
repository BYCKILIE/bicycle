//! Word Count Streaming Job with Socket I/O
//!
//! This is a complete streaming pipeline that:
//! 1. Reads lines from a TCP socket (source)
//! 2. Counts words in each line
//! 3. Writes results to a TCP socket (sink)
//!
//! # Usage
//!
//! ```bash
//! # Start the job
//! wordcount-socket --source-port 9999 --sink-port 9998
//!
//! # In another terminal, connect to receive output
//! nc localhost 9998
//!
//! # In another terminal, send input
//! nc localhost 9999
//! hello world
//! foo bar baz
//! ```
//!
//! # Output Format
//!
//! For input "hello world", output will be:
//! ```
//! hello world -> 2 words
//! ```

use anyhow::Result;
use bicycle_connectors::socket::{SocketConfig, SocketTextSink, SocketTextSource};
use bicycle_core::StreamMessage;
use bicycle_runtime::{stream_channel, Emitter};
use clap::Parser;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "wordcount-socket")]
#[command(about = "Word count streaming job with socket I/O")]
struct Args {
    /// Source port (input)
    #[arg(long, default_value = "9999")]
    source_port: u16,

    /// Sink port (output)
    #[arg(long, default_value = "9998")]
    sink_port: u16,

    /// Host to bind to
    #[arg(long, default_value = "0.0.0.0")]
    host: String,

    /// Enable aggregation mode (track cumulative word counts)
    #[arg(long)]
    aggregate: bool,

    /// Window size in seconds for aggregation (0 = no windowing)
    #[arg(long, default_value = "0")]
    window_seconds: u64,
}

/// Metrics for the job
struct JobMetrics {
    lines_processed: AtomicU64,
    words_counted: AtomicU64,
    bytes_in: AtomicU64,
    bytes_out: AtomicU64,
}

impl JobMetrics {
    fn new() -> Self {
        Self {
            lines_processed: AtomicU64::new(0),
            words_counted: AtomicU64::new(0),
            bytes_in: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
        }
    }
}

/// Word count processor
struct WordCountProcessor {
    /// Cumulative word counts (aggregation mode)
    word_counts: Arc<RwLock<HashMap<String, u64>>>,
    /// Whether to aggregate
    aggregate: bool,
    /// Metrics
    metrics: Arc<JobMetrics>,
}

impl WordCountProcessor {
    fn new(aggregate: bool, metrics: Arc<JobMetrics>) -> Self {
        Self {
            word_counts: Arc::new(RwLock::new(HashMap::new())),
            aggregate,
            metrics,
        }
    }

    /// Process a line and return the result
    async fn process(&self, line: &str) -> String {
        let words: Vec<&str> = line.split_whitespace().collect();
        let word_count = words.len();

        self.metrics.lines_processed.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .words_counted
            .fetch_add(word_count as u64, Ordering::Relaxed);
        self.metrics
            .bytes_in
            .fetch_add(line.len() as u64, Ordering::Relaxed);

        if self.aggregate {
            // Update cumulative counts
            let mut counts = self.word_counts.write().await;
            for word in &words {
                let word_lower = word.to_lowercase();
                *counts.entry(word_lower).or_insert(0) += 1;
            }

            // Format output with top words
            let mut sorted: Vec<_> = counts.iter().collect();
            sorted.sort_by(|a, b| b.1.cmp(a.1));
            let top_words: Vec<String> = sorted
                .iter()
                .take(5)
                .map(|(w, c)| format!("{}:{}", w, c))
                .collect();

            format!(
                "{} -> {} words | total unique: {} | top: [{}]",
                truncate(line, 30),
                word_count,
                counts.len(),
                top_words.join(", ")
            )
        } else {
            // Simple per-line count
            if word_count == 0 {
                format!("(empty line)")
            } else {
                let word_list: Vec<String> = words
                    .iter()
                    .take(5)
                    .map(|w| w.to_string())
                    .collect();
                let suffix = if words.len() > 5 { "..." } else { "" };
                format!(
                    "{} -> {} word{}",
                    truncate(line, 40),
                    word_count,
                    if word_count == 1 { "" } else { "s" }
                )
            }
        }
    }

    /// Get current aggregated counts
    async fn get_counts(&self) -> HashMap<String, u64> {
        self.word_counts.read().await.clone()
    }

    /// Reset counts (for windowing)
    async fn reset_counts(&self) {
        self.word_counts.write().await.clear();
    }
}

fn truncate(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len - 3])
    }
}

async fn run_pipeline(args: Args) -> Result<()> {
    let metrics = Arc::new(JobMetrics::new());
    let processor = Arc::new(WordCountProcessor::new(args.aggregate, metrics.clone()));

    // Create source
    let source_config = SocketConfig::new(&args.host, args.source_port);
    let mut source = SocketTextSource::new(source_config);

    // Create sink
    let sink_config = SocketConfig::new(&args.host, args.sink_port);
    let sink = Arc::new(SocketTextSink::new(sink_config));
    sink.start().await?;

    // Create channel for source -> processor
    let (tx, mut rx) = stream_channel::<String>(1024);
    let emitter = Emitter::new(tx);

    info!(
        source_port = args.source_port,
        sink_port = args.sink_port,
        aggregate = args.aggregate,
        "Starting word count pipeline"
    );

    println!("========================================");
    println!("  Word Count Streaming Job");
    println!("========================================");
    println!();
    println!("  Input:  nc {} {}", args.host, args.source_port);
    println!("  Output: nc {} {}", args.host, args.sink_port);
    println!();
    if args.aggregate {
        println!("  Mode: Aggregation (cumulative word counts)");
    } else {
        println!("  Mode: Per-line word count");
    }
    println!();
    println!("Waiting for connections...");
    println!();

    // Spawn source task
    tokio::spawn(async move {
        if let Err(e) = source.run(emitter).await {
            error!(error = %e, "Source error");
        }
    });

    // Spawn metrics printer
    let metrics_clone = metrics.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
        loop {
            interval.tick().await;
            let lines = metrics_clone.lines_processed.load(Ordering::Relaxed);
            let words = metrics_clone.words_counted.load(Ordering::Relaxed);
            if lines > 0 {
                info!(
                    lines_processed = lines,
                    words_counted = words,
                    "Pipeline metrics"
                );
            }
        }
    });

    // Spawn window reset task if windowing is enabled
    if args.window_seconds > 0 {
        let processor_clone = processor.clone();
        let sink_clone = sink.clone();
        let window_secs = args.window_seconds;
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(window_secs));
            loop {
                interval.tick().await;
                let counts = processor_clone.get_counts().await;
                if !counts.is_empty() {
                    // Emit window results
                    let mut sorted: Vec<_> = counts.iter().collect();
                    sorted.sort_by(|a, b| b.1.cmp(a.1));

                    let result = format!(
                        "=== WINDOW ({} sec) === {} unique words, top: {}",
                        window_secs,
                        counts.len(),
                        sorted
                            .iter()
                            .take(10)
                            .map(|(w, c)| format!("{}:{}", w, c))
                            .collect::<Vec<_>>()
                            .join(", ")
                    );

                    let _ = sink_clone.write(&result).await;
                    processor_clone.reset_counts().await;
                }
            }
        });
    }

    // Process messages
    while let Some(msg) = rx.recv().await {
        match msg {
            StreamMessage::Data(line) => {
                let result = processor.process(&line).await;

                // Send to sink
                if let Err(e) = sink.write(&result).await {
                    error!(error = %e, "Sink write error");
                }

                // Also print to console
                println!("[OUT] {}", result);
            }
            StreamMessage::Watermark(ts) => {
                info!(timestamp = ts, "Watermark received");
            }
            StreamMessage::End => {
                info!("End of stream");
                break;
            }
            _ => {}
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let args = Args::parse();
    run_pipeline(args).await
}
