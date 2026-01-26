//! Workers command - List all workers in the cluster.

use anyhow::Result;
use bicycle_protocol::control::{control_plane_client::ControlPlaneClient, ListWorkersRequest};
use tonic::transport::Channel;

pub async fn execute(client: &mut ControlPlaneClient<Channel>, format: String) -> Result<()> {
    let response = client.list_workers(ListWorkersRequest {}).await?;
    let workers = response.into_inner().workers;

    if format == "json" {
        print_json(&workers)?;
    } else {
        print_table(&workers);
    }

    Ok(())
}

fn print_table(workers: &[bicycle_protocol::control::WorkerSummary]) {
    println!("Workers ({})", workers.len());
    println!("{}", "=".repeat(90));
    println!(
        "{:<36} {:<15} {:<12} {:<12} {:<15}",
        "ID", "Hostname", "Slots", "Used", "Status"
    );
    println!("{}", "-".repeat(90));

    for w in workers {
        let status = match w.state {
            1 => "Registered",
            2 => "Active",
            3 => "Lost",
            4 => "Decommissioning",
            _ => "Unknown",
        };
        println!(
            "{:<36} {:<15} {:<12} {:<12} {:<15}",
            &w.worker_id[..w.worker_id.len().min(36)],
            &w.hostname[..w.hostname.len().min(15)],
            w.slots,
            w.slots_used,
            status
        );

        if !w.running_tasks.is_empty() {
            println!("  Tasks: {}", w.running_tasks.join(", "));
        }
    }
}

fn print_json(workers: &[bicycle_protocol::control::WorkerSummary]) -> Result<()> {
    let json: Vec<serde_json::Value> = workers
        .iter()
        .map(|w| {
            serde_json::json!({
                "worker_id": w.worker_id,
                "hostname": w.hostname,
                "slots": w.slots,
                "slots_used": w.slots_used,
                "state": w.state,
                "last_heartbeat_ms": w.last_heartbeat_ms,
                "running_tasks": w.running_tasks,
            })
        })
        .collect();

    println!("{}", serde_json::to_string_pretty(&json)?);
    Ok(())
}
