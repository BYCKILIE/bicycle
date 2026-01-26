//! Savepoint command - Trigger a savepoint for a job.

use anyhow::Result;
use bicycle_protocol::control::{
    control_plane_client::ControlPlaneClient, CancelJobRequest, TriggerCheckpointRequest,
};
use tonic::transport::Channel;

// Checkpoint type constants (from proto)
const CHECKPOINT_TYPE_SAVEPOINT: i32 = 1;

pub async fn execute(
    client: &mut ControlPlaneClient<Channel>,
    job_id: String,
    target_dir: Option<String>,
    cancel: bool,
) -> Result<()> {
    if let Some(dir) = &target_dir {
        println!("Triggering savepoint for job '{}' to '{}'...", job_id, dir);
    } else {
        println!("Triggering savepoint for job '{}'...", job_id);
    }

    // Generate a unique checkpoint ID based on timestamp
    let checkpoint_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let request = TriggerCheckpointRequest {
        job_id: job_id.clone(),
        checkpoint_id,
        timestamp,
        checkpoint_type: CHECKPOINT_TYPE_SAVEPOINT,
    };

    let response = client.trigger_checkpoint(request).await?;
    let resp = response.into_inner();

    if resp.success {
        println!();
        println!("Savepoint triggered successfully!");
        println!("  Job ID:        {}", job_id);
        println!("  Checkpoint ID: {}", checkpoint_id);
        if let Some(dir) = target_dir {
            println!("  Target Dir:    {}", dir);
        }
        println!("  Message:       {}", resp.message);

        // Cancel the job if requested
        if cancel {
            println!();
            println!("Canceling job after savepoint...");

            let cancel_request = CancelJobRequest {
                job_id: job_id.clone(),
                savepoint: false, // Already took savepoint
            };

            let cancel_response = client.cancel_job(cancel_request).await?;
            let cancel_resp = cancel_response.into_inner();

            if cancel_resp.success {
                println!("Job canceled successfully.");
            } else {
                eprintln!("Warning: Failed to cancel job: {}", cancel_resp.message);
            }
        }
    } else {
        eprintln!("Failed to trigger savepoint: {}", resp.message);
        std::process::exit(1);
    }

    Ok(())
}
