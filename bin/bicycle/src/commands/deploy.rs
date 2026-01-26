//! Deploy command - Deploy a standalone Docker-based job to run alongside the cluster.
//!
//! This command launches a job container (standalone Rust streaming job) that handles
//! its own source, processing, and sink logic. The cluster tracks the job for monitoring.

use anyhow::{Context, Result};
use bicycle_protocol::control::control_plane_client::ControlPlaneClient;
use std::process::Command;
use tonic::transport::Channel;

/// Check if Docker is available
fn check_docker() -> Result<()> {
    let output = Command::new("docker")
        .arg("--version")
        .output()
        .with_context(|| "Docker is not installed or not in PATH")?;

    if !output.status.success() {
        anyhow::bail!("Docker is not available");
    }

    Ok(())
}

/// Pull a Docker image if not present locally
fn ensure_image(image: &str, pull: bool) -> Result<()> {
    if pull {
        println!("Pulling image: {}", image);
        let status = Command::new("docker")
            .args(["pull", image])
            .status()
            .with_context(|| format!("Failed to pull image: {}", image))?;

        if !status.success() {
            anyhow::bail!("Failed to pull image: {}", image);
        }
    } else {
        // Check if image exists locally
        let output = Command::new("docker")
            .args(["image", "inspect", image])
            .output()
            .with_context(|| format!("Failed to check image: {}", image))?;

        if !output.status.success() {
            println!("Image '{}' not found locally. Use --pull to download it.", image);
            println!("Or build it with: docker build -f <Dockerfile> -t {} .", image);
            anyhow::bail!("Image not found: {}", image);
        }
    }

    Ok(())
}

/// Run a Docker container in detached mode
fn run_container(
    image: &str,
    name: &str,
    ports: &[(u16, u16)],
    env_vars: &[(String, String)],
    network: Option<&str>,
    extra_args: &[String],
) -> Result<String> {
    let mut args = vec!["run", "-d", "--name", name];

    // Add port mappings
    let port_args: Vec<String> = ports
        .iter()
        .map(|(host, container)| format!("{}:{}", host, container))
        .collect();
    for port in &port_args {
        args.push("-p");
        args.push(port);
    }

    // Add environment variables
    let env_strings: Vec<String> = env_vars
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect();
    for env in &env_strings {
        args.push("-e");
        args.push(env);
    }

    // Add network if specified
    if let Some(net) = network {
        args.push("--network");
        args.push(net);
    }

    // Add extra arguments
    for arg in extra_args {
        args.push(arg);
    }

    // Add image
    args.push(image);

    println!("Starting container: docker {}", args.join(" "));

    let output = Command::new("docker")
        .args(&args)
        .output()
        .with_context(|| "Failed to start Docker container")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("Failed to start container: {}", stderr);
    }

    let container_id = String::from_utf8_lossy(&output.stdout).trim().to_string();
    Ok(container_id)
}

/// Stop and remove a container
pub fn stop_container(name: &str) -> Result<()> {
    println!("Stopping container: {}", name);

    let _ = Command::new("docker")
        .args(["stop", name])
        .status();

    let _ = Command::new("docker")
        .args(["rm", name])
        .status();

    Ok(())
}

/// Get container logs
pub fn get_logs(name: &str, follow: bool) -> Result<()> {
    let mut args = vec!["logs"];
    if follow {
        args.push("-f");
    }
    args.push(name);

    let status = Command::new("docker")
        .args(&args)
        .status()
        .with_context(|| format!("Failed to get logs for: {}", name))?;

    if !status.success() {
        anyhow::bail!("Container not found: {}", name);
    }

    Ok(())
}

pub async fn execute(
    _client: &mut ControlPlaneClient<Channel>,
    image: String,
    name: Option<String>,
    source_port: u16,
    sink_port: u16,
    pull: bool,
    network: Option<String>,
    detach: bool,
) -> Result<()> {
    // Check Docker is available
    check_docker()?;

    // Ensure image is available
    ensure_image(&image, pull)?;

    // Generate container name if not provided
    let container_name = name.unwrap_or_else(|| {
        let short_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        format!("bicycle-job-{}", short_id)
    });

    // Define ports
    let ports = vec![
        (source_port, source_port),
        (sink_port, sink_port),
    ];

    // Environment variables
    let env_vars = vec![
        ("SOURCE_PORT".to_string(), source_port.to_string()),
        ("SINK_PORT".to_string(), sink_port.to_string()),
    ];

    // Extra args (pass port args to the job)
    let extra_args = vec![
        format!("--source-port={}", source_port),
        format!("--sink-port={}", sink_port),
    ];

    // Start the container
    let container_id = run_container(
        &image,
        &container_name,
        &ports,
        &env_vars,
        network.as_deref(),
        &extra_args,
    )?;

    println!();
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║                    Job Deployed                            ║");
    println!("╠════════════════════════════════════════════════════════════╣");
    println!("║  Container: {:44} ║", &container_id[..12.min(container_id.len())]);
    println!("║  Name:      {:44} ║", container_name);
    println!("║  Image:     {:44} ║", &image[..44.min(image.len())]);
    println!("╠════════════════════════════════════════════════════════════╣");
    println!("║  Input:  nc localhost {:5}                                ║", source_port);
    println!("║  Output: nc localhost {:5}                                ║", sink_port);
    println!("╚════════════════════════════════════════════════════════════╝");
    println!();

    if !detach {
        println!("Attaching to logs (Ctrl+C to detach)...");
        println!();
        get_logs(&container_name, true)?;
    } else {
        println!("Container running in background.");
        println!();
        println!("Commands:");
        println!("  View logs:  docker logs -f {}", container_name);
        println!("  Stop job:   docker stop {}", container_name);
        println!("  Remove:     docker rm {}", container_name);
    }

    Ok(())
}
