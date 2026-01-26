//! Cancel command - Cancel a running job.

use anyhow::Result;
use bicycle_protocol::control::{control_plane_client::ControlPlaneClient, CancelJobRequest};
use tonic::transport::Channel;

pub async fn execute(
    client: &mut ControlPlaneClient<Channel>,
    job_id: String,
    force: bool,
) -> Result<()> {
    println!("Canceling job '{}'...", job_id);

    let request = CancelJobRequest {
        job_id: job_id.clone(),
        savepoint: !force, // Take savepoint unless force is specified
    };

    let response = client.cancel_job(request).await?;
    let resp = response.into_inner();

    if resp.success {
        println!();
        println!("Job canceled successfully!");
        println!("  Job ID:  {}", job_id);
        println!("  Message: {}", resp.message);
        if !resp.savepoint_path.is_empty() {
            println!("  Savepoint: {}", resp.savepoint_path);
        }
    } else {
        eprintln!("Failed to cancel job: {}", resp.message);
        std::process::exit(1);
    }

    Ok(())
}
