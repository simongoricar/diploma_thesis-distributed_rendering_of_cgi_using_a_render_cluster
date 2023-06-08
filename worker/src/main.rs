mod cli;
mod connection;
mod rendering;
mod utilities;

use clap::Parser;
use log::info;
use miette::{miette, Context, IntoDiagnostic, Result};
use shared::results::worker_trace::WorkerTraceBuilder;

use crate::cli::CLIArgs;
use crate::connection::Worker;
use crate::rendering::runner::BlenderJobRunner;
use crate::utilities::parse_with_tilde_support;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::try_init().into_diagnostic()?;

    let args = CLIArgs::parse();

    let master_server_address = format!(
        "{}:{}",
        args.master_server_host, args.master_server_port
    );

    info!("Initializing BlenderJobRunner.");

    let blender_binary_path = parse_with_tilde_support(&args.blender_binary_path)
        .wrap_err_with(|| miette!("Could not parse blender binary path with tilde support."))?;
    let base_directory_path = parse_with_tilde_support(&args.base_directory_path)
        .wrap_err_with(|| miette!("Could not parse base directory path with tilde support."))?;

    let tracer = WorkerTraceBuilder::new_empty();
    let runner = BlenderJobRunner::new(
        blender_binary_path,
        args.blender_prepend_arguments.unwrap_or_default(),
        base_directory_path,
        tracer.clone(),
    )?;

    info!("Running worker until job is complete.");

    Worker::connect_and_run_to_job_completion(&master_server_address, runner, tracer)
        .await
        .wrap_err_with(|| miette!("Errored while running worker"))?;

    Ok(())
}
