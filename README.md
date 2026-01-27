# Bicycle

A **Rust-first distributed streaming engine** inspired by Apache Flink.

Bicycle provides exactly-once stream processing semantics with a clean, idiomatic Rust API and production-grade state management via RocksDB.

## Features

### Streaming API
- **Fluent DataStream API**: Build pipelines with a Flink-like fluent interface
- **User-defined functions**: `AsyncFunction` (stateless) and `RichAsyncFunction` (stateful)
- **Type-safe pipelines**: Compile-time type checking for streaming operations
- **Keyed streams**: Key-based partitioning for aggregations and stateful processing
- **Operator UIDs**: Stable identifiers for state recovery across job restarts
- **Graph optimization**: Automatic operator chaining and slot sharing

### Native Plugin System
- **Native performance**: Compile jobs as shared libraries (.so/.dylib/.dll)
- **Dynamic loading**: Workers load plugins at runtime via `libloading`
- **Stateful plugins**: Support for checkpointed state in plugins
- **Simple macro**: `bicycle_plugin!` generates all FFI exports automatically

### Core Capabilities
- **Streaming operators**: Map, Filter, FlatMap, KeyBy, Reduce
- **Window operations**: Tumbling, Sliding, Session windows
- **Event-time processing**: Watermarks, late event handling
- **Backpressure**: Bounded channels with credit-based flow control
- **State management**: Memory and RocksDB state backends
- **Checkpointing**: Chandy-Lamport distributed snapshots with barrier alignment
- **Network layer**: TCP-based data plane with efficient serialization

### Distributed Coordination
- **JobManager**: Full control plane with gRPC API for job submission, scheduling, and monitoring
- **Worker nodes**: Task executors with heartbeat-based health monitoring and plugin loading
- **Task scheduling**: Slot-based allocation with locality-aware placement
- **Cluster monitoring**: Real-time metrics, worker status, and job tracking

### Exactly-Once Semantics
- **Transactional sinks**: Two-phase commit protocol for exactly-once delivery
- **Idempotent writes**: Deduplication tracking for sink operations
- **Checkpoint coordination**: Barrier alignment with timeout-based fallback

### Connectors
- **Apache Kafka**: Source and sink with exactly-once transactional support
- **Socket (TCP)**: Test connectors for development and debugging
- **Apache Pulsar**: Source and sink with sequence-based deduplication (placeholder)

### Web UI & CLI
- **Dashboard**: Real-time cluster overview with job and worker status
- **Job management**: Submit, monitor, and cancel jobs via REST API
- **Metrics**: Records processed, throughput, checkpoints, and resource usage
- **Bicycle CLI**: Full-featured command-line interface for cluster management
- **Plugin deployment**: Submit jobs with native plugins or Docker images

---

## Quick Start

### Option 1: Docker (Recommended)

```bash
# Start the full cluster (JobManager + 2 Workers + Web UI)
docker compose up -d

# Wait for cluster to be ready, then check status
docker compose --profile cli run --rm cli status

# List available jobs
docker compose --profile cli run --rm cli jobs

# Submit the wordcount job
docker compose --profile cli run --rm cli run jobs/wordcount.yaml

# Connect to the job (in separate terminals)
nc localhost 9998    # Terminal 1: Receive output
nc localhost 9999    # Terminal 2: Send input (type "hello world")

# Open the Web UI
open http://localhost:8081

# View logs
docker compose logs -f

# Stop the cluster
docker compose down
```

### Option 2: Local Development

#### Prerequisites

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install -y \
    build-essential \
    cmake \
    clang \
    libclang-dev \
    llvm-dev \
    libsnappy-dev \
    liblz4-dev \
    libzstd-dev \
    zlib1g-dev

# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

**macOS:**
```bash
# Install Xcode command line tools
xcode-select --install

# Install dependencies via Homebrew
brew install cmake llvm snappy lz4 zstd

# Set LLVM path (add to ~/.zshrc or ~/.bashrc)
export LIBCLANG_PATH="$(brew --prefix llvm)/lib"
export PATH="$(brew --prefix llvm)/bin:$PATH"

# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

**Fedora/RHEL:**
```bash
sudo dnf install -y \
    gcc gcc-c++ \
    cmake \
    clang clang-devel \
    llvm-devel \
    snappy-devel \
    lz4-devel \
    libzstd-devel \
    zlib-devel
```

**Arch Linux:**
```bash
sudo pacman -S base-devel cmake clang llvm snappy lz4 zstd
```

#### Build and Run

```bash
# Clone the repository
git clone https://github.com/yourusername/bicycle.git
cd bicycle

# Build (first build takes a while due to RocksDB)
cargo build --release

# Run the local demo pipeline (standalone, no cluster needed)
cargo run -p mini-runner

# Run with debug logging
RUST_LOG=debug cargo run -p mini-runner
```

Expected output:
```
INFO  Starting Bicycle streaming demo
window=[0..5000] key=a sum=4
window=[0..5000] key=b sum=2
window=[5000..10000] key=a sum=16
window=[5000..10000] key=b sum=4
window=[10000..15000] key=a sum=6
window=[10000..15000] key=b sum=2
window=[15000..20000] key=b sum=8
INFO  Demo complete
```

---

## Bicycle CLI

The `bicycle` command-line tool is the primary interface for interacting with a Bicycle cluster.

### Commands

```bash
bicycle [--jobmanager <ADDR>] <COMMAND>

Commands:
  run        Submit a job from a YAML definition file
  submit     Submit a Docker-based or native executable job
  deploy     Deploy a standalone Docker-based job
  jobs       List available job definitions
  list       List all running/completed jobs
  info       Get detailed information about a job
  cancel     Cancel a running job
  savepoint  Trigger a savepoint for a job
  status     Show cluster status overview
  workers    List all workers in the cluster
  metrics    Show cluster and job metrics
```

### Examples

```bash
# Check cluster status
./target/release/bicycle status

# List available job definitions
./target/release/bicycle jobs

# Submit a job from a YAML file
./target/release/bicycle run jobs/wordcount.yaml

# Submit with overrides
./target/release/bicycle run jobs/wordcount.yaml --name my-wordcount -p 4

# Submit with a native plugin
./target/release/bicycle submit my-job --plugin target/release/libwordcount_plugin.so

# Submit a Docker-based job
./target/release/bicycle submit my-job --docker --image my-job:latest

# Deploy a standalone Docker job
./target/release/bicycle deploy my-job:latest --source-port 9999 --sink-port 9998

# List all running jobs
./target/release/bicycle list

# List only running jobs (as JSON)
./target/release/bicycle list --state running --format json

# Get job details
./target/release/bicycle info <job-id>

# Cancel a job (with savepoint)
./target/release/bicycle cancel <job-id>

# Force cancel (no savepoint)
./target/release/bicycle cancel <job-id> --force

# Trigger savepoint
./target/release/bicycle savepoint <job-id> --target-dir /tmp/savepoints

# View metrics
./target/release/bicycle metrics
```

### Docker Usage

```bash
# List available jobs
docker compose --profile cli run --rm cli jobs

# Submit a job
docker compose --profile cli run --rm cli run jobs/wordcount.yaml

# Check cluster status
docker compose --profile cli run --rm cli status

# List running jobs
docker compose --profile cli run --rm cli list

# List workers
docker compose --profile cli run --rm cli workers --format json
```

---

## Testing Socket Connectors

Bicycle includes TCP socket connectors for testing and development. The `socket-test` utility helps you verify these connectors work correctly.

### Socket Test Commands

```bash
socket-test <COMMAND>

Commands:
  source    Run a socket source server that prints received lines
  sink      Run a socket sink server that accepts connections
  echo      Run an echo pipeline: source -> transform -> sink
  generate  Generate test data and send to a sink
  loopback  Run a simple loopback test (source and sink on same process)
```

### Quick Loopback Test

The fastest way to verify socket connectors work:

```bash
# Build and run loopback test
cargo build --release --bin socket-test
./target/release/socket-test loopback --count 5
```

Expected output:
```
Loopback test:
  Source port: 9999
  Sink port:   9998
  Messages:    5
  [1] test-message-0
  [2] test-message-1
  [3] test-message-2
  [4] test-message-3
  [5] test-message-4
Loopback test passed: 5 messages
```

### Interactive Echo Pipeline

Test with netcat to see data flow through the pipeline:

```bash
# Terminal 1: Start the echo pipeline
./target/release/socket-test echo --source-port 9999 --sink-port 9998

# Terminal 2: Connect to receive output
nc localhost 9998

# Terminal 3: Send input
nc localhost 9999
# Type messages and see them appear in Terminal 2
```

With transformation:
```bash
# Echo with uppercase transformation
./target/release/socket-test echo --transform upper

# Available transforms: none, upper, lower, reverse
```

### Source Server (receive data)

```bash
# Start a source that prints received lines
./target/release/socket-test source --port 9999

# In another terminal, send data
echo "hello world" | nc localhost 9999
```

### Sink Server (send data)

```bash
# Start a sink that broadcasts to connected clients
./target/release/socket-test sink --port 9998

# In another terminal, receive data
nc localhost 9998

# Type messages in the sink terminal to send to all clients
```

### Generate Test Data

```bash
# Generate 100 messages at 100ms intervals
./target/release/socket-test generate --host localhost --port 9998 --count 100 --interval 100

# Generate infinite messages (Ctrl+C to stop)
./target/release/socket-test generate --count 0 --interval 1000 --prefix "event"
```

### Docker Usage

```bash
# Run echo pipeline in Docker
docker compose --profile test run socket-test echo

# Run loopback test
docker compose --profile test run socket-test loopback --count 10
```

---

## Submitting Jobs to the Cluster

Bicycle follows a Flink-like architecture where jobs are submitted to the JobManager and executed on Workers. Jobs are defined as YAML files in the `jobs/` directory.

### Job Definition Files

Jobs are defined in YAML files that describe the pipeline:

```yaml
# jobs/wordcount.yaml
name: wordcount
description: "Counts words from socket input"

config:
  parallelism: 1
  max_parallelism: 8       # Max parallelism for rescaling (default: parallelism * 4)
  checkpoint_interval_ms: 60000

vertices:
  - id: source
    name: "Socket Source"
    operator: source
    connector:
      type: socket
      properties:
        port: "9999"

  - id: flatmap
    name: "Split Words"
    operator: flatmap
    config:
      function: split_words

  - id: count
    name: "Word Count"
    operator: count

  - id: sink
    name: "Socket Sink"
    operator: sink
    connector:
      type: socket
      properties:
        port: "9998"

edges:
  - from: source
    to: flatmap
  - from: flatmap
    to: count
  - from: count
    to: sink
```

**With Custom Plugin Functions:**
```yaml
# jobs/wordcount-custom.yaml
name: wordcount-custom
description: "Word count using custom Rust AsyncFunction implementations"

config:
  parallelism: 1
  checkpoint_interval_ms: 60000

vertices:
  - id: source
    name: "Socket Source"
    operator: source
    connector:
      type: socket
      properties:
        host: "0.0.0.0"
        port: "9999"

  - id: splitter
    name: "WordSplitter"
    operator: process       # Uses plugin function

  - id: counter
    name: "WordCounter"
    operator: process       # Uses plugin function

  - id: sink
    name: "Socket Sink"
    operator: sink
    connector:
      type: socket
      properties:
        host: "0.0.0.0"
        port: "9998"

edges:
  - from: source
    to: splitter
  - from: splitter
    to: counter
  - from: counter
    to: sink
```

### Submitting Jobs

```bash
# 1. Start the cluster
docker compose up -d

# 2. List available jobs
docker compose --profile cli run --rm cli jobs

# 3. Submit the wordcount job
docker compose --profile cli run --rm cli run jobs/wordcount.yaml

# Output:
# Submitting job 'wordcount' from jobs/wordcount.yaml...
# Pipeline: Socket Source -> Split Words -> Word Count -> Socket Sink
# Job submitted successfully!
#   Job ID: abc123...

# 4. Connect to the job (worker-1 exposes ports 9999 and 9998)
# Terminal 2: Receive word counts
nc localhost 9998

# Terminal 3: Send text input
nc localhost 9999
hello world
hello bicycle
```

**Output format:**
```
hello:1
world:1
hello:2
bicycle:1
```

### Job Architecture

Jobs are defined as a DAG of operators:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Source    │ ──▶ │  FlatMap    │ ──▶ │   Count     │ ──▶ │    Sink     │
│  (Socket)   │     │(split_words)│     │ (by word)   │     │  (Socket)   │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
```

**Available Operators:**
- **Source**: Read from connectors (Socket, Kafka, Generator)
- **Map**: Transform each element (uppercase, lowercase, word_count, reverse)
- **FlatMap**: Transform to zero or more elements (split_words, split_lines, split_csv)
- **Filter**: Keep elements matching predicates (non_empty, contains, starts_with)
- **KeyBy**: Partition by key
- **Window**: Group into time windows
- **Count**: Count elements by key
- **Reduce**: Aggregate elements
- **Sink**: Write to connectors (Socket, Kafka, Console)

**Available Connectors:**
- **Socket**: TCP socket source/sink for testing
- **Kafka**: Production-grade Kafka source/sink
- **Generator**: Built-in data generator for testing
- **Console**: Print to stdout (sink only)

### Local Development (Standalone)

For quick local testing without the cluster:

```bash
# Build the standalone word count binary
cargo build --release --bin wordcount-socket

# Run locally (not on cluster)
./target/release/wordcount-socket --source-port 9999 --sink-port 9998

# Connect with netcat as above
```

---

## Building Native Plugins

Native plugins allow you to write streaming jobs in Rust and compile them to shared libraries that workers load dynamically.

### Plugin Project Structure

```
jobs/my-plugin/
├── Cargo.toml
└── src/
    └── lib.rs
```

**Cargo.toml:**
```toml
[package]
name = "my-plugin"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]  # Build as shared library

[dependencies]
bicycle-api = { path = "../../crates/api" }
bicycle-plugin = { path = "../../crates/plugin" }
async-trait = "0.1"
serde = { version = "1", features = ["derive"] }
```

**src/lib.rs:**
```rust
use bicycle_api::prelude::*;
use bicycle_plugin::bicycle_plugin;

/// A stateless function that converts text to uppercase.
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

/// A stateless function that splits lines into words.
#[derive(Default)]
pub struct WordSplitter;

#[async_trait]
impl AsyncFunction for WordSplitter {
    type In = String;
    type Out = String;

    async fn process(&mut self, input: String, _ctx: &Context) -> Vec<String> {
        input.split_whitespace()
            .map(|w| w.to_lowercase())
            .filter(|w| !w.is_empty())
            .collect()
    }

    fn name(&self) -> &str {
        "WordSplitter"
    }
}

// Export all functions
bicycle_plugin!(ToUppercase, WordSplitter);
```

### Build and Deploy

```bash
# Build the plugin
cargo build --release -p my-plugin

# The shared library is at:
# Linux:   target/release/libmy_plugin.so
# macOS:   target/release/libmy_plugin.dylib
# Windows: target/release/my_plugin.dll

# Submit to cluster with plugin
./target/release/bicycle submit my-job --plugin target/release/libmy_plugin.so

# Or reference in job YAML
./target/release/bicycle run jobs/my-job.yaml
```

### Stateful Plugins

For functions that need to maintain state across elements:

```rust
use bicycle_api::prelude::*;
use bicycle_plugin::bicycle_plugin_rich;
use std::collections::HashMap;

#[derive(Default)]
pub struct WordCounter {
    counts: HashMap<String, u64>,
}

#[async_trait]
impl RichAsyncFunction for WordCounter {
    type In = String;
    type Out = String;

    async fn open(&mut self, _ctx: &RuntimeContext) -> Result<()> {
        // Initialize or restore state
        Ok(())
    }

    async fn process(&mut self, word: String, _ctx: &RuntimeContext) -> Vec<String> {
        let count = self.counts.entry(word.clone()).or_insert(0);
        *count += 1;
        vec![format!("{}:{}", word, count)]
    }

    async fn snapshot(&self, _ctx: &RuntimeContext) -> Result<()> {
        // Save state for checkpointing
        Ok(())
    }

    fn name(&self) -> &str {
        "WordCounter"
    }
}

// Export stateful functions with bicycle_plugin_rich!
bicycle_plugin_rich!(WordCounter);
```

---

## Demo Tutorial

This tutorial walks you through running a complete Bicycle cluster.

### Step 1: Start the Cluster

```bash
# Start all services
docker compose up -d

# Verify all containers are running
docker compose ps
```

You should see:
```
NAME                 STATUS
bicycle-jobmanager   Up (healthy)
bicycle-worker-1     Up
bicycle-worker-2     Up
bicycle-webui        Up
```

### Step 2: Open the Web UI

Navigate to http://localhost:8081 in your browser. You'll see:

- **Cluster Overview**: Workers count, available slots, running jobs
- **Workers Table**: List of registered workers with status
- **Jobs Table**: Running and completed jobs

### Step 3: Use the Bicycle CLI

```bash
# Check cluster status
docker compose --profile cli run --rm cli status

# List workers
docker compose --profile cli run --rm cli workers

# List available job definitions
docker compose --profile cli run --rm cli jobs

# Submit a job
docker compose --profile cli run --rm cli run jobs/wordcount.yaml

# List running jobs
docker compose --profile cli run --rm cli list

# Get job details
docker compose --profile cli run --rm cli info <job-id>

# View metrics
docker compose --profile cli run --rm cli metrics
```

### Step 4: Use the REST API

```bash
# Get cluster info
curl http://localhost:8081/api/cluster

# List workers
curl http://localhost:8081/api/cluster/workers

# List jobs
curl http://localhost:8081/api/jobs

# Get metrics
curl http://localhost:8081/api/metrics

# Submit a job (via POST)
curl -X POST http://localhost:8081/api/jobs \
    -H "Content-Type: application/json" \
    -d '{"name": "my-job", "parallelism": 2}'
```

### Step 5: Clean Up

```bash
docker compose down -v  # -v removes volumes
```

---

## Running a Cluster (Manual)

### Start the JobManager

```bash
cargo run -p jobmanager -- --bind 0.0.0.0:9000 --checkpoint-dir /tmp/bicycle/checkpoints
```

### Start Worker(s)

```bash
# Worker 1
cargo run -p worker -- --jobmanager 127.0.0.1:9000 --slots 4

# Worker 2 (on another terminal or machine)
cargo run -p worker -- --jobmanager 127.0.0.1:9000 --slots 4
```

### Start the Web UI

```bash
cargo run -p bicycle-webui -- --bind 0.0.0.0:8081 --jobmanager 127.0.0.1:9000
```

Then open http://localhost:8081 in your browser.

### Use the Bicycle CLI

```bash
# Build the CLI
cargo build --release -p bicycle

# Check status
./target/release/bicycle --jobmanager 127.0.0.1:9000 status

# List available job definitions
./target/release/bicycle jobs

# Submit a job from YAML file
./target/release/bicycle --jobmanager 127.0.0.1:9000 run jobs/wordcount.yaml

# List running jobs
./target/release/bicycle list
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Job Manager                          │
│  ┌─────────────┐  ┌──────────────┐  ┌───────────────────┐   │
│  │ Job Graph   │  │  Scheduler   │  │   Checkpoint      │   │
│  │ Management  │  │              │  │   Coordinator     │   │
│  └─────────────┘  └──────────────┘  └───────────────────┘   │
│  ┌─────────────┐  ┌──────────────┐                          │
│  │  Metrics    │  │   Worker     │                          │
│  │  Collector  │  │   Registry   │                          │
│  └─────────────┘  └──────────────┘                          │
└─────────────────────────────────────────────────────────────┘
              gRPC (port 9000)  │             │ REST (port 8081)
                                │             │
                                │      ┌──────┴──────┐
                                │      │   Web UI    │
                                │      │  Dashboard  │
                                │      └─────────────┘
                                │
     ┌──────────────────────────┼──────────────────────────┐
     ▼                          ▼                          ▼
┌───────────────────┐ ┌───────────────────┐ ┌───────────────────┐
│     Worker 1      │ │     Worker 2      │ │     Worker 3      │
│  ┌─────────────┐  │ │  ┌─────────────┐  │ │  ┌─────────────┐  │
│  │   Task 1    │  │ │  │   Task 2    │  │ │  │   Task 3    │  │
│  │  (Source)   │──┼─┼─▶│  (Window)   │──┼─┼─▶│   (Sink)    │  │
│  └─────────────┘  │ │  └─────────────┘  │ │  └─────────────┘  │
│  ┌─────────────┐  │ │  ┌─────────────┐  │ │                   │
│  │  RocksDB    │  │ │  │  RocksDB    │  │ │                   │
│  │   State     │  │ │  │   State     │  │ │                   │
│  └─────────────┘  │ │  └─────────────┘  │ │                   │
└───────────────────┘ └───────────────────┘ └───────────────────┘
```

---

## Project Structure

```
bicycle/
├── crates/
│   ├── core/           # Core types: StreamMessage, Event, Window, TaskId
│   ├── runtime/        # Operator trait, channels, task execution, transactional sinks
│   ├── operators/      # Built-in operators (Map, Filter, Window, etc.)
│   ├── state/          # State backends (Memory, RocksDB)
│   ├── checkpoint/     # Checkpoint coordination and barrier tracking
│   ├── network/        # TCP data plane, serialization, flow control
│   ├── protocol/       # gRPC generated code from protobuf definitions
│   ├── connectors/     # External system connectors (Kafka, Socket)
│   ├── api/            # User-facing DataStream API (like Flink's API)
│   ├── api-macros/     # Procedural macros for the API
│   └── plugin/         # Native plugin support (.so/.dylib/.dll)
├── bin/
│   ├── mini-runner/    # Local demo pipeline (standalone)
│   ├── jobmanager/     # Control plane server (gRPC)
│   ├── worker/         # Task executor with heartbeat and plugin loading
│   ├── webui/          # Web dashboard and REST API
│   ├── bicycle/        # CLI for cluster management
│   ├── socket-test/    # Socket connector test utility
│   └── wordcount-socket/ # Standalone wordcount (local testing, no cluster)
├── jobs/               # Job definition files (YAML)
│   ├── wordcount.yaml  # Word count streaming job
│   ├── wordcount-custom.yaml # Word count with custom Rust functions
│   ├── wordcount-plugin/ # Example native plugin project
│   └── echo.yaml       # Echo/uppercase job
├── test-jobs/          # Test job implementations
│   └── wordcount/      # Standalone wordcount binary
├── proto/              # gRPC service definitions
│   ├── control.proto   # Control plane RPC (job/task/cluster management)
│   └── data.proto      # Data plane messages
├── config/             # Configuration files
├── Dockerfile          # Multi-stage Docker build
└── docker-compose.yml  # Full cluster setup
```

---

## Usage Examples

### Streaming API (Recommended)

The `bicycle-api` crate provides a fluent DataStream API similar to Apache Flink:

```rust
use bicycle_api::prelude::*;

// Define a custom function
#[derive(Default)]
struct WordSplitter;

#[async_trait]
impl AsyncFunction for WordSplitter {
    type In = String;
    type Out = String;

    async fn process(&mut self, input: String, _ctx: &Context) -> Vec<String> {
        input.split_whitespace()
            .map(|w| w.to_lowercase())
            .collect()
    }
}

// Build a streaming pipeline with configuration
fn main() -> Result<()> {
    let env = StreamEnvironment::builder()
        .parallelism(2)              // Default parallelism for all operators
        .max_parallelism(16)         // Max parallelism for rescaling
        .checkpoint_interval(30_000) // Checkpoint every 30 seconds
        .build();

    env.socket_source("0.0.0.0", 9999)
        .process(WordSplitter)
        .set_parallelism(4)  // Override: this operator runs with parallelism 4
        .key_by(|word| word.clone())
        .count()
        .socket_sink("0.0.0.0", 9998);

    env.execute("wordcount")
}
```

**Available DataStream Operations:**
- **map** / **flat_map** / **filter** - Transform elements
- **process** - Apply custom `AsyncFunction`
- **process_rich** - Apply stateful `RichAsyncFunction`
- **process_plugin** - Load function from native plugin
- **key_by** - Partition by key for keyed operations
- **count** / **reduce** - Aggregations
- **tumbling_window** / **sliding_window** / **session_window** - Windowing
- **socket_sink** / **kafka_sink** / **print** - Output sinks

**Operator Configuration:**
- **uid(id)** - Set stable identifier for state recovery across restarts
- **name(name)** - Set display name (shown in UI)
- **set_parallelism(n)** - Override parallelism for an operator
- **set_max_parallelism(n)** - Override max parallelism for rescaling
- **slot_sharing_group(name)** - Assign to slot sharing group
- **disable_chaining()** - Force task boundary (prevent operator fusion)

**Environment Configuration:**
- **parallelism(n)** - Default parallelism for all operators
- **max_parallelism(n)** - Maximum parallelism for rescaling
- **checkpoint_interval(ms)** - Interval between checkpoints

### Graph Optimization

The job graph optimizer automatically fuses operators to reduce task overhead:

```rust
// Build and optimize the job graph
let (graph, optimized) = env.execute_optimized("my-job")?;

// Print optimization summary
optimized.print_summary();
// Output:
// === Job Graph Optimization Summary ===
// Job: my-job
// Operators: 6 -> 2 chains
// Tasks: 12 -> 4 (66.7% reduction)
// Slot sharing groups: 1
// Minimum slots needed: 2
```

**Operator Chaining Rules:**
- Same parallelism
- Forward partitioning (not hash/rebalance/broadcast)
- Same slot sharing group
- Chaining not disabled on either operator

```rust
// Operators with same parallelism are chained automatically
env.socket_source("0.0.0.0", 9999)  // p=2
    .map(|x| x.to_uppercase())      // p=2, chained with source
    .filter(|x| !x.is_empty())      // p=2, chained
    .key_by(|x| x.clone())          // Forces new chain (hash partitioning)
    .count()
    .socket_sink("0.0.0.0", 9998);

// Force a chain break
stream.map(|x| x)
    .disable_chaining()  // Next operator starts new chain
    .filter(|x| x > 0);
```

**Slot Sharing:**
Operators in the same slot sharing group can share task slots:

```rust
// Default group - these can share slots
stream.map(|x| x).slot_sharing_group("default");

// Separate group - isolated slots for heavy processing
stream.process(HeavyFunction).slot_sharing_group("heavy");
```

### Native Plugin System

Build streaming jobs as native shared libraries for maximum performance:

```rust
// jobs/my-plugin/src/lib.rs
use bicycle_api::prelude::*;
use bicycle_plugin::bicycle_plugin;

#[derive(Default)]
pub struct ToUppercase;

#[async_trait]
impl AsyncFunction for ToUppercase {
    type In = String;
    type Out = String;

    async fn process(&mut self, input: String, _ctx: &Context) -> Vec<String> {
        vec![input.to_uppercase()]
    }
}

// Export the function
bicycle_plugin!(ToUppercase);
```

Build and deploy:
```bash
# Build the plugin as a shared library
cargo build --release -p my-plugin
# Result: target/release/libmy_plugin.so

# Submit with the plugin
bicycle submit my-job --plugin target/release/libmy_plugin.so
```

**Plugin Features:**
- Full native performance (no interpretation overhead)
- Access to system libraries and I/O
- Stateful functions with `RichAsyncFunction` and `bicycle_plugin_rich!`
- Automatic state checkpointing and recovery
- Hot reloading support (planned)

### Stateful Functions

Use `RichAsyncFunction` for functions that maintain state:

```rust
use bicycle_api::prelude::*;
use std::collections::HashMap;

#[derive(Default)]
struct WordCounter {
    counts: HashMap<String, u64>,
}

#[async_trait]
impl RichAsyncFunction for WordCounter {
    type In = String;
    type Out = String;

    async fn open(&mut self, ctx: &RuntimeContext) -> Result<()> {
        // Restore state from checkpoint if available
        if let Some(state) = ctx.get_state("counts")? {
            self.counts = state;
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
}
```

### Low-Level Operators

For advanced use cases, you can use the runtime directly:

```rust
use bicycle_operators::{MapOperator, FilterOperator, TumblingWindowSum};
use bicycle_runtime::{spawn_operator, stream_channel};

// Create channels
let (tx1, rx1) = stream_channel::<Event<String, i64>>(32);
let (tx2, rx2) = stream_channel::<Event<String, i64>>(32);

// Filter: keep only positive values
spawn_operator(
    "filter",
    FilterOperator::new(|ev: &Event<String, i64>| ev.value > 0),
    rx1,
    tx2,
);

// Window: 5-second tumbling sum
spawn_operator(
    "window",
    TumblingWindowSum::new(5_000),
    rx2,
    tx3,
);
```

### Socket Connectors

```rust
use bicycle_connectors::socket::{SocketTextSource, SocketTextSink, SocketConfig};
use bicycle_runtime::Emitter;

// Create a socket source listening on port 9999
let config = SocketConfig::server(9999);
let mut source = SocketTextSource::new(config);

// Run the source with an emitter
source.run(emitter).await?;

// Create a socket sink listening on port 9998
let sink = SocketTextSink::server(9998);
sink.start().await?;

// Write events as lines
sink.write("hello world").await?;
sink.write("foo bar").await?;
```

### State Management

```rust
use bicycle_state::{RocksDBStateBackend, ValueState, MapState};

// Create RocksDB backend
let backend = RocksDBStateBackend::new("/var/bicycle/state")?;
let store = backend.create_keyed_state_store("my-operator")?;

// Set current key (like Flink's KeyedProcessFunction)
store.set_current_key(b"user-123");

// Use ValueState
let counter: Box<dyn ValueState<i64>> = store.get_value_state("counter")?;
counter.set(42)?;
assert_eq!(counter.get()?, Some(42));

// Use MapState
let scores: Box<dyn MapState<String, i64>> = store.get_map_state("scores")?;
scores.put("game1".into(), 100)?;
scores.put("game2".into(), 200)?;
```

### Checkpointing

```rust
use bicycle_checkpoint::{CheckpointCoordinator, CheckpointConfig};
use std::time::Duration;

let config = CheckpointConfig {
    interval: Duration::from_secs(10),
    timeout: Duration::from_secs(600),
    num_retained: 3,
    ..Default::default()
};

let coordinator = CheckpointCoordinator::new(job_id, config, checkpoint_dir);

// Trigger checkpoint
let checkpoint_id = coordinator.trigger_checkpoint(false)?;

// Trigger savepoint (for upgrades/migrations)
let savepoint_id = coordinator.trigger_checkpoint(true)?;
```

### Kafka Connector

```rust
use bicycle_connectors::kafka::{KafkaSource, KafkaSink, KafkaConfig};

// Configure Kafka source
let source_config = KafkaConfig::new("localhost:9092")
    .with_group_id("my-consumer-group")
    .with_topics(vec!["input-topic".to_string()]);

let mut source = KafkaSource::new(source_config);
source.connect()?;

// Configure Kafka sink with exactly-once
let sink = KafkaSinkBuilder::new("localhost:9092")
    .topic("output-topic")
    .exactly_once("bicycle-sink")
    .build_transactional()?;
```

### Exactly-Once Sink

```rust
use bicycle_runtime::sink::{TransactionalSink, ExactlyOnceSinkWriter};

// Wrap any transactional sink for exactly-once semantics
let (commit_tx, commit_rx) = tokio::sync::mpsc::channel(16);
let writer = ExactlyOnceSinkWriter::new(sink, commit_tx);

// Process messages - transactions are managed automatically
writer.process(StreamMessage::Data(record)).await?;

// On checkpoint completion, commit pending transactions
writer.notify_checkpoint_complete(checkpoint_id).await?;
```

---

## Configuration

### JobManager (`config/jobmanager.yaml`)
```yaml
bind: "0.0.0.0:9000"
checkpoint_interval_ms: 10000
state_backend: "rocksdb"
state_path: "/var/bicycle/state"
checkpoint_path: "/var/bicycle/checkpoints"
```

### Worker (`config/worker.yaml`)
```yaml
jobmanager: "localhost:9000"
bind: "0.0.0.0:9001"
slots: 4
memory_mb: 4096
state_path: "/var/bicycle/state"
```

### Job Configuration (YAML)

| Property | Description | Default |
|----------|-------------|---------|
| `parallelism` | Default parallelism for all operators | `1` |
| `max_parallelism` | Maximum parallelism for rescaling (must be >= parallelism) | `parallelism * 4` |
| `checkpoint_interval_ms` | Interval between checkpoints | `60000` |
| `max_restarts` | Maximum restart attempts on failure | `3` |
| `restart_delay_ms` | Delay between restart attempts | `5000` |

**Per-vertex parallelism** can also be set on individual operators:
```yaml
vertices:
  - id: source
    operator: source
    parallelism: 2    # Override for this operator only
```

### Environment Variables
| Variable | Description | Default |
|----------|-------------|---------|
| `RUST_LOG` | Log level | `info` |
| `BICYCLE_STATE_DIR` | State storage path | `/var/bicycle/state` |
| `BICYCLE_CHECKPOINT_DIR` | Checkpoint path | `/var/bicycle/checkpoints` |
| `BICYCLE_JOBMANAGER` | JobManager address | `localhost:9000` |
| `BICYCLE_WORKER_SLOTS` | Task slots per worker | `4` |
| `BICYCLE_WORKER_MEMORY_MB` | Worker memory limit | `2048` |

---

## REST API

The Web UI provides a REST API for job and cluster management:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/jobs` | GET | List all jobs |
| `/api/jobs` | POST | Submit a new job |
| `/api/jobs/{id}` | GET | Get job details |
| `/api/jobs/{id}` | DELETE | Cancel a job |
| `/api/jobs/{id}/tasks` | GET | Get tasks for a job |
| `/api/jobs/{id}/checkpoints` | GET | Get checkpoint history |
| `/api/cluster` | GET | Get cluster info (workers, slots, jobs) |
| `/api/cluster/workers` | GET | List workers with metrics |
| `/api/metrics` | GET | Get cluster metrics (throughput, records) |
| `/health` | GET | Health check |

### Example Responses

**GET /api/cluster**
```json
{
  "workers": 2,
  "total_slots": 8,
  "available_slots": 1,
  "running_jobs": 1
}
```

**GET /api/cluster/workers**
```json
[
  {
    "worker_id": "abc12345-...",
    "hostname": "worker-1",
    "slots": 4,
    "slots_used": 3,
    "cpu_usage": 45.2,
    "memory_used_mb": 512
  }
]
```

**GET /api/metrics**
```json
{
  "total_records_processed": 1250000,
  "total_bytes_processed": 52428800,
  "checkpoints_completed": 12,
  "uptime_seconds": 3600
}
```

---

## Development

```bash
# Build all crates
cargo build

# Run tests
cargo test

# Run specific test
cargo test -p bicycle-state

# Run with verbose logging
RUST_LOG=bicycle=debug cargo run -p mini-runner

# Test socket connectors
cargo run -p socket-test -- loopback --count 10

# Format code
cargo fmt

# Lint
cargo clippy --all-targets

# Build documentation
cargo doc --open
```

---

## Troubleshooting

### Build Errors

**`libclang not found`**
```bash
# Ubuntu/Debian
sudo apt-get install libclang-dev

# macOS
brew install llvm
export LIBCLANG_PATH="$(brew --prefix llvm)/lib"
```

**`snappy/lz4/zstd not found`**
```bash
# Ubuntu/Debian
sudo apt-get install libsnappy-dev liblz4-dev libzstd-dev

# macOS
brew install snappy lz4 zstd
```

**Slow builds**

RocksDB and protobuf compilation is slow on first build. Subsequent builds use cached artifacts.

```bash
# Use more parallelism
CARGO_BUILD_JOBS=8 cargo build --release
```

### Docker Issues

**Workers not connecting**
```bash
# Check if JobManager is healthy
docker compose ps
docker compose logs jobmanager

# Verify network connectivity
docker compose exec worker-1 nc -z jobmanager 9000
```

**CLI not working**
```bash
# Make sure to use the profile flag
docker compose --profile cli run cli status
```

---

## Roadmap

### Phase 1: Core Completion ✅
- [x] JobManager with gRPC API
- [x] Worker registration and heartbeat
- [x] Task scheduling with slot allocation
- [x] Web UI dashboard
- [x] Cluster monitoring and metrics
- [x] Bicycle CLI (replaced demo-client)
- [x] Socket connectors for testing
- [x] Task deployment from JobManager to Workers
- [x] Operator execution on Workers (demo operators)
- [x] Data plane communication setup (network layer)

### Phase 1.5: Developer Experience ✅
- [x] Fluent DataStream API (`bicycle-api` crate)
- [x] User-defined functions (`AsyncFunction`, `RichAsyncFunction`)
- [x] Native plugin system (`bicycle-plugin` crate)
- [x] Plugin loading in workers (`libloading`)
- [x] CLI support for plugin and Docker deployments
- [x] Example plugin project (`jobs/wordcount-plugin`)

### Phase 2: Production Features
- [ ] Checkpoint persistence and recovery
- [ ] Savepoint creation and restore
- [ ] Job restart on failure
- [ ] Backpressure propagation across network
- [ ] Dynamic scaling (add/remove workers)
- [ ] Resource isolation per task

### Phase 3: Advanced Features
- [ ] **SQL support** via Apache Calcite or DataFusion
- [ ] **More connectors**: Kinesis, RabbitMQ, Redis, PostgreSQL CDC
- [ ] **Auto-scaling** based on backpressure metrics
- [ ] **Savepoint management UI**
- [ ] **Job versioning** and rolling upgrades
- [ ] **Multi-tenancy** with resource quotas
- [ ] **Hot plugin reloading**

### Phase 4: Enterprise Features
- [ ] High availability (HA) JobManager
- [ ] Kubernetes operator
- [ ] Prometheus metrics exporter
- [ ] Grafana dashboards
- [ ] Authentication and authorization
- [ ] Audit logging

### Contributing

Contributions are welcome! Priority areas:
1. Checkpoint persistence and job recovery
2. Additional connectors (Kinesis, RabbitMQ, etc.)
3. SQL support via DataFusion
4. Documentation and examples

---

## Comparison with Apache Flink

| Feature | Flink | Bicycle |
|---------|-------|---------|
| Language | Java/Scala | Rust |
| DataStream API | Yes | Yes (fluent API) |
| User-defined Functions | Yes | Yes (AsyncFunction, RichAsyncFunction) |
| State Backend | RocksDB, Memory | RocksDB, Memory |
| Checkpointing | Chandy-Lamport | Chandy-Lamport |
| Event Time | Yes | Yes |
| Exactly-Once | Yes | Yes |
| Kafka Connector | Yes | Yes |
| Web UI | Yes | Yes |
| Native Plugins | No (JVM only) | Yes (.so/.dylib/.dll) |
| SQL Support | Yes | Planned |
| Memory Safety | JVM GC | Rust ownership |
| Startup Time | Seconds | Milliseconds |
| Binary Size | ~100MB+ | ~20MB |

---

## License

Apache-2.0

---

## Acknowledgments

Inspired by:
- [Apache Flink](https://flink.apache.org/)
- [Timely Dataflow](https://github.com/TimelyDataflow/timely-dataflow)
- [Arroyo](https://github.com/ArroyoSystems/arroyo)
