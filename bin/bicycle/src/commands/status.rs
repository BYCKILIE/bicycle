//! Status command - Show cluster status overview.

use anyhow::Result;
use bicycle_protocol::control::{control_plane_client::ControlPlaneClient, GetClusterInfoRequest};
use tonic::transport::Channel;

pub async fn execute(client: &mut ControlPlaneClient<Channel>) -> Result<()> {
    let response = client.get_cluster_info(GetClusterInfoRequest {}).await?;
    let info = response.into_inner();

    println!("Bicycle Cluster Status");
    println!("{}", "=".repeat(40));
    println!();
    println!("Workers:      {}/{} active", info.active_workers, info.total_workers);
    println!("Slots:        {}/{} available", info.available_slots, info.total_slots);
    println!("Running Jobs: {}", info.running_jobs);
    println!("Total Tasks:  {}", info.total_tasks);
    println!("Uptime:       {}s", info.uptime_ms / 1000);

    Ok(())
}
