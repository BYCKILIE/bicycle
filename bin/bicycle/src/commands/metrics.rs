//! Metrics command - Show cluster and job metrics.

use anyhow::Result;
use bicycle_protocol::control::{
    control_plane_client::ControlPlaneClient, GetJobStatusRequest, GetMetricsRequest,
};
use tonic::transport::Channel;

pub async fn execute(
    client: &mut ControlPlaneClient<Channel>,
    job_id: Option<String>,
) -> Result<()> {
    if let Some(job_id) = job_id {
        // Show metrics for a specific job
        show_job_metrics(client, job_id).await
    } else {
        // Show cluster-wide metrics
        show_cluster_metrics(client).await
    }
}

async fn show_cluster_metrics(client: &mut ControlPlaneClient<Channel>) -> Result<()> {
    let response = client.get_metrics(GetMetricsRequest {}).await?;
    let m = response.into_inner();

    println!("Cluster Metrics");
    println!("{}", "=".repeat(50));
    println!();
    println!("Uptime:              {}s", m.uptime_seconds);
    println!("Records Processed:   {}", m.total_records_processed);
    println!("Bytes Processed:     {}", format_bytes(m.total_bytes_processed));
    println!("Checkpoints OK:      {}", m.checkpoints_completed);
    println!("Checkpoints Failed:  {}", m.checkpoints_failed);
    println!();
    println!("Throughput");
    println!("{}", "-".repeat(40));
    println!("  Records: {:.2} records/s", m.throughput_records_per_sec);
    println!("  Bytes:   {}/s", format_bytes(m.throughput_bytes_per_sec as i64));

    if !m.job_records.is_empty() {
        println!();
        println!("Per-Job Records");
        println!("{}", "-".repeat(40));
        for (job_id, records) in m.job_records {
            println!("  {}: {} records", &job_id[..job_id.len().min(20)], records);
        }
    }

    Ok(())
}

async fn show_job_metrics(client: &mut ControlPlaneClient<Channel>, job_id: String) -> Result<()> {
    let response = client
        .get_job_status(GetJobStatusRequest {
            job_id: job_id.clone(),
        })
        .await?;
    let job = response.into_inner();

    println!("Job Metrics: {}", job_id);
    println!("{}", "=".repeat(50));
    println!();

    if let Some(metrics) = job.metrics {
        println!("Records In:          {}", metrics.records_in);
        println!("Records Out:         {}", metrics.records_out);
        println!("Bytes In:            {}", format_bytes(metrics.bytes_in));
        println!("Bytes Out:           {}", format_bytes(metrics.bytes_out));
        println!();
        println!("Checkpoints");
        println!("{}", "-".repeat(40));
        println!("  Last Checkpoint ID:   {}", metrics.last_checkpoint_id);
        println!("  Last Checkpoint Time: {}ms ago", metrics.last_checkpoint_time);
    } else {
        println!("No metrics available for this job.");
    }

    if !job.task_statuses.is_empty() {
        println!();
        println!("Task Metrics ({})", job.task_statuses.len());
        println!("{}", "-".repeat(50));
        println!("{:<25} {:<12} {:<12}", "Task", "Records", "Bytes");

        for task in job.task_statuses {
            println!(
                "{:<25} {:<12} {:<12}",
                &task.task_id[..task.task_id.len().min(25)],
                task.records_processed,
                format_bytes(task.bytes_processed)
            );
        }
    }

    Ok(())
}

fn format_bytes(bytes: i64) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.1} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}
