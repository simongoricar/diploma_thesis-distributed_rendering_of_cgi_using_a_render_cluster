mod cli;
mod connection;
mod rendering;
mod utilities;

use clap::Parser;
use log::info;
use miette::{miette, Context, IntoDiagnostic, Result};
use url::Url;

use crate::cli::CLIArgs;
use crate::connection::Worker;
use crate::rendering::runner::BlenderJobRunner;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::try_init().into_diagnostic()?;

    let args = CLIArgs::parse();

    let master_server_address = Url::parse(&format!(
        "ws://{}:{}",
        args.master_server_host, args.master_server_port
    ))
    .into_diagnostic()
    .wrap_err_with(|| miette!("Invalid master server address."))?;

    info!("Initializing ClientWorker.");
    let client_worker = Worker::connect(master_server_address)
        .await
        .wrap_err_with(|| miette!("Could not connect to master server."))?;

    info!("Initializing BlenderJobRunner.");
    let runner = BlenderJobRunner::new(
        args.blender_binary_path.clone(),
        args.base_directory_path.clone(),
    )?;

    info!("Running worker indefinitely.");
    client_worker
        .run_to_job_completion(runner)
        .await
        .wrap_err_with(|| miette!("Errored while running worker"))?;

    Ok(())
}
