mod cli;
mod connection;
mod rendering;
mod utilities;

use clap::Parser;
use log::info;
use miette::{miette, Context, IntoDiagnostic, Result};

use crate::cli::CLIArgs;
use crate::connection::Worker;
use crate::utilities::BlenderJobRunner;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::try_init().into_diagnostic()?;

    let args = CLIArgs::parse();

    let server_address = url::Url::parse("ws://127.0.0.1:9901").into_diagnostic()?;

    info!("Initializing ClientWorker.");
    let client_worker = Worker::connect(server_address)
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
