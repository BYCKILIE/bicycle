//! Socket connector test utility.
//!
//! This binary tests the socket source and sink connectors for development
//! and debugging purposes.
//!
//! # Examples
//!
//! ```bash
//! # Start a source server (reads from netcat clients)
//! socket-test source --port 9999
//! # In another terminal: nc localhost 9999, then type lines
//!
//! # Start a sink server (writes to netcat clients)
//! socket-test sink --port 9998
//! # In another terminal: nc localhost 9998 to see output
//!
//! # Run an echo pipeline (source -> sink)
//! socket-test echo --source-port 9999 --sink-port 9998
//! # In terminals: nc localhost 9999 (input), nc localhost 9998 (output)
//!
//! # Generate test data to a sink
//! socket-test generate --port 9998 --count 100 --interval 100
//! ```

use anyhow::Result;
use bicycle_connectors::socket::{SocketConfig, SocketTextSink, SocketTextSource};
use bicycle_core::StreamMessage;
use bicycle_runtime::{stream_channel, Emitter};
use clap::{Parser, Subcommand};
use std::time::Duration;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "socket-test")]
#[command(about = "Test utility for Bicycle socket connectors")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run a socket source server that prints received lines
    Source {
        /// Port to listen on
        #[arg(long, short, default_value = "9999")]
        port: u16,

        /// Run as client instead of server
        #[arg(long)]
        client: bool,

        /// Host to connect to (client mode only)
        #[arg(long, default_value = "localhost")]
        host: String,
    },

    /// Run a socket sink server that accepts connections
    Sink {
        /// Port to listen on
        #[arg(long, short, default_value = "9998")]
        port: u16,

        /// Run as client instead of server
        #[arg(long)]
        client: bool,

        /// Host to connect to (client mode only)
        #[arg(long, default_value = "localhost")]
        host: String,
    },

    /// Run an echo pipeline: source -> transform -> sink
    Echo {
        /// Source port (input)
        #[arg(long, default_value = "9999")]
        source_port: u16,

        /// Sink port (output)
        #[arg(long, default_value = "9998")]
        sink_port: u16,

        /// Transform mode: none, upper, lower, reverse
        #[arg(long, default_value = "none")]
        transform: String,
    },

    /// Generate test data and send to a sink
    Generate {
        /// Port to connect to
        #[arg(long, short, default_value = "9998")]
        port: u16,

        /// Host to connect to
        #[arg(long, default_value = "localhost")]
        host: String,

        /// Number of messages to generate (0 = infinite)
        #[arg(long, short, default_value = "0")]
        count: u64,

        /// Interval between messages in milliseconds
        #[arg(long, short, default_value = "1000")]
        interval: u64,

        /// Message prefix
        #[arg(long, default_value = "message")]
        prefix: String,
    },

    /// Run a simple loopback test (source and sink on same process)
    Loopback {
        /// Source port
        #[arg(long, default_value = "9999")]
        source_port: u16,

        /// Sink port
        #[arg(long, default_value = "9998")]
        sink_port: u16,

        /// Number of messages to send
        #[arg(long, short, default_value = "10")]
        count: u64,
    },
}

async fn cmd_source(port: u16, client: bool, host: String) -> Result<()> {
    let (tx, mut rx) = stream_channel::<String>(1024);
    let emitter = Emitter::new(tx);

    let config = if client {
        info!(host = %host, port = port, "Starting socket source (client mode)");
        SocketConfig::client(host, port)
    } else {
        info!(port = port, "Starting socket source (server mode)");
        println!("Listening on port {}. Use 'nc localhost {}' to send data.", port, port);
        SocketConfig::server(port)
    };

    let mut source = SocketTextSource::new(config);

    // Spawn source in background
    tokio::spawn(async move {
        if let Err(e) = source.run(emitter).await {
            error!(error = %e, "Source error");
        }
    });

    // Print received messages
    println!("\nReceived messages:");
    println!("{}", "-".repeat(60));

    while let Some(msg) = rx.recv().await {
        match msg {
            StreamMessage::Data(line) => {
                println!("{}", line);
            }
            StreamMessage::Watermark(ts) => {
                info!(timestamp = ts, "Watermark");
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

async fn cmd_sink(port: u16, client: bool, host: String) -> Result<()> {
    let config = if client {
        info!(host = %host, port = port, "Starting socket sink (client mode)");
        SocketConfig::client(host, port)
    } else {
        info!(port = port, "Starting socket sink (server mode)");
        println!("Listening on port {}. Use 'nc localhost {}' to receive data.", port, port);
        SocketConfig::server(port)
    };

    let sink = SocketTextSink::new(config);
    sink.start().await?;

    println!("\nType messages to send (Ctrl+C to exit):");
    println!("{}", "-".repeat(60));

    // Read from stdin and write to sink
    use tokio::io::AsyncBufReadExt;
    let stdin = tokio::io::stdin();
    let reader = tokio::io::BufReader::new(stdin);
    let mut lines = reader.lines();

    while let Ok(Some(line)) = lines.next_line().await {
        sink.write(&line).await?;
        info!(clients = sink.client_count(), "Sent: {}", line);
    }

    Ok(())
}

async fn cmd_echo(source_port: u16, sink_port: u16, transform: String) -> Result<()> {
    info!(
        source_port = source_port,
        sink_port = sink_port,
        transform = %transform,
        "Starting echo pipeline"
    );

    println!("Echo pipeline started:");
    println!("  Input:  nc localhost {}", source_port);
    println!("  Output: nc localhost {}", sink_port);
    println!("  Transform: {}", transform);
    println!();

    // Create source
    let (tx, mut rx) = stream_channel::<String>(1024);
    let emitter = Emitter::new(tx);
    let mut source = SocketTextSource::new(SocketConfig::server(source_port));

    // Create sink
    let sink = SocketTextSink::new(SocketConfig::server(sink_port));
    sink.start().await?;

    // Spawn source
    tokio::spawn(async move {
        if let Err(e) = source.run(emitter).await {
            error!(error = %e, "Source error");
        }
    });

    // Process messages
    let mut count = 0u64;
    while let Some(msg) = rx.recv().await {
        if let StreamMessage::Data(line) = msg {
            let output = match transform.as_str() {
                "upper" => line.to_uppercase(),
                "lower" => line.to_lowercase(),
                "reverse" => line.chars().rev().collect(),
                _ => line,
            };

            if let Err(e) = sink.write(&output).await {
                error!(error = %e, "Sink error");
            }

            count += 1;
            if count % 100 == 0 {
                info!(count = count, clients = sink.client_count(), "Processed messages");
            }
        }
    }

    Ok(())
}

async fn cmd_generate(host: String, port: u16, count: u64, interval: u64, prefix: String) -> Result<()> {
    info!(
        host = %host,
        port = port,
        count = count,
        interval = interval,
        prefix = %prefix,
        "Starting message generator"
    );

    let sink = SocketTextSink::new(SocketConfig::client(host, port));
    sink.start().await?;

    println!("Generating messages...");

    let mut i = 0u64;
    loop {
        let msg = format!("{}-{}", prefix, i);
        sink.write(&msg).await?;
        println!("Sent: {}", msg);

        i += 1;
        if count > 0 && i >= count {
            break;
        }

        tokio::time::sleep(Duration::from_millis(interval)).await;
    }

    println!("\nGenerated {} messages", i);
    Ok(())
}

async fn cmd_loopback(source_port: u16, sink_port: u16, count: u64) -> Result<()> {
    info!(
        source_port = source_port,
        sink_port = sink_port,
        count = count,
        "Starting loopback test"
    );

    println!("Loopback test:");
    println!("  Source port: {}", source_port);
    println!("  Sink port:   {}", sink_port);
    println!("  Messages:    {}", count);
    println!();

    // Start sink server first
    let sink = SocketTextSink::new(SocketConfig::server(sink_port));
    sink.start().await?;

    // Start source server
    let (tx, mut rx) = stream_channel::<String>(1024);
    let emitter = Emitter::new(tx);
    let mut source = SocketTextSource::new(SocketConfig::server(source_port));

    tokio::spawn(async move {
        if let Err(e) = source.run(emitter).await {
            error!(error = %e, "Source error");
        }
    });

    // Give servers time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect a client to the source and send messages
    let sender = tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;
        use tokio::net::TcpStream;

        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut stream = TcpStream::connect(format!("localhost:{}", source_port)).await?;
        for i in 0..count {
            let msg = format!("test-message-{}\n", i);
            stream.write_all(msg.as_bytes()).await?;
        }
        stream.flush().await?;
        Result::<_, anyhow::Error>::Ok(())
    });

    // Receive and forward to sink
    let mut received = 0u64;
    let timeout = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(msg) = rx.recv().await {
            if let StreamMessage::Data(line) = msg {
                sink.write(&line).await?;
                received += 1;
                println!("  [{}] {}", received, line);

                if received >= count {
                    break;
                }
            }
        }
        Result::<_, anyhow::Error>::Ok(())
    });

    match timeout.await {
        Ok(Ok(())) => {
            println!("\nLoopback test passed: {} messages", received);
        }
        Ok(Err(e)) => {
            error!(error = %e, "Test failed");
            return Err(e);
        }
        Err(_) => {
            println!("\nTest timed out after receiving {} messages", received);
        }
    }

    sender.abort();
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let args = Args::parse();

    match args.command {
        Command::Source { port, client, host } => {
            cmd_source(port, client, host).await?;
        }
        Command::Sink { port, client, host } => {
            cmd_sink(port, client, host).await?;
        }
        Command::Echo {
            source_port,
            sink_port,
            transform,
        } => {
            cmd_echo(source_port, sink_port, transform).await?;
        }
        Command::Generate {
            port,
            host,
            count,
            interval,
            prefix,
        } => {
            cmd_generate(host, port, count, interval, prefix).await?;
        }
        Command::Loopback {
            source_port,
            sink_port,
            count,
        } => {
            cmd_loopback(source_port, sink_port, count).await?;
        }
    }

    Ok(())
}
