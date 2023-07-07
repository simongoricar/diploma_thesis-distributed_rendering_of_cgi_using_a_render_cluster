mod cli;
mod connection;
mod rendering;
mod utilities;

use clap::Parser;
use miette::{miette, Context, Result};
use shared::logging::initialize_console_and_file_logging;
use shared::results::worker_trace::WorkerTraceBuilder;
use tracing::info;

use crate::cli::CLIArgs;
use crate::connection::Worker;
use crate::rendering::runner::BlenderJobRunner;
use crate::utilities::parse_with_tilde_support;


#[tokio::main]
async fn main() -> Result<()> {
    let args = CLIArgs::parse();

    let _guard = initialize_console_and_file_logging(args.log_output_file_path.as_ref());


    let master_server_address = format!(
        "{}:{}",
        args.master_server_host, args.master_server_port
    );

    info!("Initializing BlenderJobRunner.");

    let blender_binary_path = parse_with_tilde_support(&args.blender_binary)
        .wrap_err_with(|| miette!("Could not parse blender binary path with tilde support."))?;
    let base_directory_path = parse_with_tilde_support(&args.base_directory_path)
        .wrap_err_with(|| miette!("Could not parse base directory path with tilde support."))?;

    let tracer = WorkerTraceBuilder::new_empty();
    let runner = BlenderJobRunner::new(
        blender_binary_path,
        args.blender_prepend_arguments,
        args.blender_append_arguments,
        base_directory_path,
        tracer.clone(),
    )?;

    info!("Running worker until job is complete.");

    Worker::connect_and_run_to_job_completion(&master_server_address, runner, tracer)
        .await
        .wrap_err_with(|| miette!("Errored while running worker"))?;

    Ok(())
}
