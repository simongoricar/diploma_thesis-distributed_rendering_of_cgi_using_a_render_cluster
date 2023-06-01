mod cli;
mod jobs;
mod worker;

use clap::Parser;
use log::info;
use miette::{miette, Context, IntoDiagnostic, Result};

use crate::cli::CLIArgs;
use crate::jobs::BlenderJobRunner;
use crate::worker::ClientWorker;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::try_init().into_diagnostic()?;

    let args = CLIArgs::parse();

    info!("Initializing ClientWorker.");
    let client_worker = ClientWorker::connect()
        .await
        .wrap_err_with(|| miette!("Could not connect to master server."))?;

    info!("Initializing BlenderJobRunner.");
    let runner = BlenderJobRunner::new(
        args.blender_binary_path.clone(),
        args.base_directory_path.clone(),
    )?;

    info!("Running worker indefinitely.");
    client_worker
        .run_forever(runner)
        .await
        .wrap_err_with(|| miette!("Errored while running worker"))?;

    Ok(())
}
