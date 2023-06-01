pub mod cli;
pub mod cluster;
pub mod connection;

use clap::Parser;
use log::info;
use miette::{miette, Context, IntoDiagnostic, Result};
use shared::jobs::BlenderJob;

use crate::cli::{CLIArgs, CLICommand};
use crate::cluster::ClusterManager;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::try_init().into_diagnostic()?;

    let args = CLIArgs::parse();

    #[allow(irrefutable_let_patterns)]
    if let CLICommand::RunJob(run_job_args) = args.command {
        info!("Loading job file.");
        let job = BlenderJob::load_from_file(run_job_args.job_file_path)
            .wrap_err_with(|| miette!("Could not load Blender job from file."))?;

        info!("Initializing cluster manager.");
        let mut manager = ClusterManager::new_from_job(job)
            .await
            .wrap_err_with(|| miette!("Could not initialize cluster manager."))?;

        info!("Running server to job completion.");
        manager
            .run_job_to_completion()
            .await
            .wrap_err_with(|| miette!("Could not run server and job to completion."))?;
    }

    Ok(())
}
