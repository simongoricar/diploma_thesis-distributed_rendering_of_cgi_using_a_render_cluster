mod cli;
mod manager;
mod websockets;

use clap::Parser;
use log::info;
use miette::{IntoDiagnostic, Result};
use shared::jobs::BlenderJob;

use crate::cli::{CLIArgs, CLICommand};
use crate::manager::ClusterManager;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::try_init().into_diagnostic()?;

    let args = CLIArgs::parse();

    #[allow(irrefutable_let_patterns)]
    if let CLICommand::RunJob(run_job_args) = args.command {
        info!("Loading job file.");
        let job = BlenderJob::load_from_file(run_job_args.job_file_path)?;

        info!("Initializing cluster manager.");
        let mut manager = ClusterManager::new_from_job(job).await?;

        info!("Running server to job completion.");
        manager.run_server_and_job_to_completion().await?;
    }

    Ok(())
}
