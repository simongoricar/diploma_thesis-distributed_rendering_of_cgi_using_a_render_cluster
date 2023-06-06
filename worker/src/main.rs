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
use crate::utilities::parse_with_tilde_support;

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

    let blender_binary_path = parse_with_tilde_support(&args.blender_binary_path)
        .wrap_err_with(|| miette!("Could not parse blender binary path with tilde support."))?;
    let base_directory_path = parse_with_tilde_support(&args.base_directory_path)
        .wrap_err_with(|| miette!("Could not parse base directory path with tilde support."))?;

    let runner = BlenderJobRunner::new(blender_binary_path, base_directory_path)?;

    info!("Running worker indefinitely.");
    client_worker
        .run_to_job_completion(runner)
        .await
        .wrap_err_with(|| miette!("Errored while running worker"))?;

    Ok(())
}
