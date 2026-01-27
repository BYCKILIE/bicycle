//! REST API server for Bicycle Web UI.

use anyhow::Result;
use axum::{
    routing::{delete, get, post},
    Router,
};
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::info;

use crate::handlers;

/// Application state shared across handlers.
pub struct AppState {
    pub jobmanager_addr: String,
}

/// Run the web server.
pub async fn run_server(bind: &str, jobmanager: &str) -> Result<()> {
    let state = Arc::new(AppState {
        jobmanager_addr: jobmanager.to_string(),
    });

    let app = Router::new()
        // Job management
        .route("/api/jobs", get(handlers::list_jobs))
        .route("/api/jobs", post(handlers::submit_job))
        .route("/api/jobs/:job_id", get(handlers::get_job))
        .route("/api/jobs/:job_id", delete(handlers::cancel_job))
        .route("/api/jobs/:job_id/tasks", get(handlers::get_job_tasks))
        .route("/api/jobs/:job_id/checkpoints", get(handlers::get_job_checkpoints))
        .route("/api/jobs/:job_id/graph", get(handlers::get_job_graph))
        .route("/api/jobs/:job_id/exceptions", get(handlers::get_job_exceptions))
        // Cluster info
        .route("/api/cluster", get(handlers::get_cluster_info))
        .route("/api/cluster/workers", get(handlers::list_workers))
        // Metrics
        .route("/api/metrics", get(handlers::get_metrics))
        .route("/api/metrics/history", get(handlers::get_metrics_history))
        // Health
        .route("/health", get(handlers::health_check))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind).await?;
    info!("API server listening on {}", bind);

    axum::serve(listener, app).await?;
    Ok(())
}
