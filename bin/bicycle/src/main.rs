//! Bicycle CLI - Command-line interface for the Bicycle streaming engine.
//!
//! This is the primary client tool for interacting with a Bicycle cluster,
//! similar to how `flink` CLI works for Apache Flink.
//!
//! # Usage
//!
//! ```bash
//! # Show cluster status
//! bicycle status
//!
//! # List all jobs
//! bicycle list
//!
//! # Submit a job from a definition file
//! bicycle run jobs/wordcount.yaml
//!
//! # Get job details
//! bicycle info <job-id>
//!
//! # Cancel a job
//! bicycle cancel <job-id>
//! ```

mod commands;

use anyhow::Result;
use bicycle_protocol::control::control_plane_client::ControlPlaneClient;
use clap::{Parser, Subcommand};
use tonic::transport::Channel;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "bicycle")]
#[command(about = "Bicycle CLI - Command-line interface for the Bicycle streaming engine")]
#[command(version)]
pub struct Args {
    /// JobManager address (can also be set via BICYCLE_JOBMANAGER env var)
    #[arg(long, short = 'j', env = "BICYCLE_JOBMANAGER", default_value = "127.0.0.1:9000", global = true)]
    pub jobmanager: String,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Submit a job to the cluster from a job definition file
    Run {
        /// Path to job definition file (YAML)
        job_file: String,

        /// Override job name
        #[arg(long, short = 'n')]
        name: Option<String>,

        /// Override parallelism for all operators
        #[arg(long, short = 'p')]
        parallelism: Option<i32>,
    },

    /// List all jobs
    List {
        /// Filter by job state (running, finished, failed, cancelled)
        #[arg(long, short = 's')]
        state: Option<String>,

        /// Output format (table, json)
        #[arg(long, short = 'f', default_value = "table")]
        format: String,
    },

    /// Get detailed information about a job
    Info {
        /// Job ID
        job_id: String,

        /// Output format (table, json)
        #[arg(long, short = 'f', default_value = "table")]
        format: String,
    },

    /// Cancel a running job
    Cancel {
        /// Job ID
        job_id: String,

        /// Force cancellation without savepoint
        #[arg(long)]
        force: bool,
    },

    /// Trigger a savepoint for a job
    Savepoint {
        /// Job ID
        job_id: String,

        /// Target directory for the savepoint
        #[arg(long, short = 'd')]
        target_dir: Option<String>,

        /// Cancel the job after taking the savepoint
        #[arg(long)]
        cancel: bool,
    },

    /// Show cluster status overview
    Status,

    /// List all workers in the cluster
    Workers {
        /// Output format (table, json)
        #[arg(long, short = 'f', default_value = "table")]
        format: String,
    },

    /// Show cluster and job metrics
    Metrics {
        /// Filter metrics by job ID
        #[arg(long)]
        job_id: Option<String>,
    },

    /// List available job definitions
    Jobs {
        /// Directory containing job definitions
        #[arg(long, short = 'd', default_value = "jobs")]
        dir: String,
    },

    /// Submit a Docker-based or native executable job to the cluster
    Submit {
        /// Path to job executable or Docker image name
        executable: String,

        /// Override job name
        #[arg(long, short = 'n')]
        name: Option<String>,

        /// Override parallelism for all operators
        #[arg(long, short = 'p')]
        parallelism: Option<i32>,

        /// Run as Docker container instead of native executable
        #[arg(long)]
        docker: bool,

        /// Docker image name (if different from executable)
        #[arg(long)]
        image: Option<String>,

        /// Path to native plugin (.so) file for plugin-based jobs
        #[arg(long)]
        plugin: Option<String>,
    },

    /// Deploy a standalone Docker-based job (runs custom Rust streaming logic)
    Deploy {
        /// Docker image name for the job
        image: String,

        /// Container name (auto-generated if not specified)
        #[arg(long, short = 'n')]
        name: Option<String>,

        /// Source port (for receiving input data)
        #[arg(long, default_value = "9999")]
        source_port: u16,

        /// Sink port (for sending output data)
        #[arg(long, default_value = "9998")]
        sink_port: u16,

        /// Pull image from registry before deploying
        #[arg(long)]
        pull: bool,

        /// Docker network to connect to
        #[arg(long)]
        network: Option<String>,

        /// Run in detached mode (don't follow logs)
        #[arg(long, short = 'd')]
        detach: bool,
    },
}

pub async fn get_client(addr: &str) -> Result<ControlPlaneClient<Channel>> {
    let addr = format!("http://{}", addr);
    let client = ControlPlaneClient::connect(addr).await?;
    Ok(client)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("warn".parse()?))
        .init();

    let args = Args::parse();

    match args.command {
        Command::Run {
            job_file,
            name,
            parallelism,
        } => {
            let mut client = get_client(&args.jobmanager).await?;
            commands::run::execute(&mut client, job_file, name, parallelism).await?;
        }
        Command::List { state, format } => {
            let mut client = get_client(&args.jobmanager).await?;
            commands::list::execute(&mut client, state, format).await?;
        }
        Command::Info { job_id, format } => {
            let mut client = get_client(&args.jobmanager).await?;
            commands::info::execute(&mut client, job_id, format).await?;
        }
        Command::Cancel { job_id, force } => {
            let mut client = get_client(&args.jobmanager).await?;
            commands::cancel::execute(&mut client, job_id, force).await?;
        }
        Command::Savepoint {
            job_id,
            target_dir,
            cancel,
        } => {
            let mut client = get_client(&args.jobmanager).await?;
            commands::savepoint::execute(&mut client, job_id, target_dir, cancel).await?;
        }
        Command::Status => {
            let mut client = get_client(&args.jobmanager).await?;
            commands::status::execute(&mut client).await?;
        }
        Command::Workers { format } => {
            let mut client = get_client(&args.jobmanager).await?;
            commands::workers::execute(&mut client, format).await?;
        }
        Command::Metrics { job_id } => {
            let mut client = get_client(&args.jobmanager).await?;
            commands::metrics::execute(&mut client, job_id).await?;
        }
        Command::Jobs { dir } => {
            commands::jobs::execute(dir)?;
        }
        Command::Submit {
            executable,
            name,
            parallelism,
            docker,
            image,
            plugin,
        } => {
            let mut client = get_client(&args.jobmanager).await?;
            commands::submit::execute(&mut client, executable, name, parallelism, docker, image, plugin).await?;
        }
        Command::Deploy {
            image,
            name,
            source_port,
            sink_port,
            pull,
            network,
            detach,
        } => {
            let mut client = get_client(&args.jobmanager).await?;
            commands::deploy::execute(&mut client, image, name, source_port, sink_port, pull, network, detach).await?;
        }
    }

    Ok(())
}
