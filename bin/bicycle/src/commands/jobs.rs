//! Jobs command - List available job definitions.

use anyhow::Result;
use std::fs;
use std::path::Path;

use super::run::load_job_definition;

pub fn execute(dir: String) -> Result<()> {
    let path = Path::new(&dir);

    if !path.exists() {
        eprintln!("Jobs directory not found: {}", dir);
        eprintln!();
        eprintln!("Create a jobs directory with YAML job definitions:");
        eprintln!("  mkdir jobs");
        eprintln!("  # Add job files like jobs/wordcount.yaml");
        std::process::exit(1);
    }

    if !path.is_dir() {
        eprintln!("Not a directory: {}", dir);
        std::process::exit(1);
    }

    let entries: Vec<_> = fs::read_dir(path)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map(|ext| ext == "yaml" || ext == "yml")
                .unwrap_or(false)
        })
        .collect();

    if entries.is_empty() {
        println!("No job definitions found in {}", dir);
        println!();
        println!("Create YAML job definition files in the jobs directory.");
        println!("Example: jobs/wordcount.yaml");
        return Ok(());
    }

    println!("Available Jobs");
    println!("==============================================================================");
    println!("{:<20} {:<50}", "NAME", "DESCRIPTION");
    println!("------------------------------------------------------------------------------");

    for entry in entries {
        let path = entry.path();
        match load_job_definition(&path) {
            Ok(job) => {
                let desc = if job.description.len() > 48 {
                    format!("{}...", &job.description[..45])
                } else {
                    job.description.clone()
                };
                println!("{:<20} {:<50}", job.name, desc);
            }
            Err(e) => {
                let filename = path.file_name()
                    .map(|s| s.to_string_lossy().to_string())
                    .unwrap_or_default();
                eprintln!("{:<20} (error: {})", filename, e);
            }
        }
    }

    println!();
    println!("Submit a job: bicycle run {}/JOB_NAME.yaml", dir);

    Ok(())
}
