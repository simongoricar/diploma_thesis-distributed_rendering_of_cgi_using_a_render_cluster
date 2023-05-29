mod worker;

use log::info;
use miette::{IntoDiagnostic, Result};

use crate::worker::ClientWorker;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::try_init().into_diagnostic()?;

    info!("Initializing ClientWorker.");
    let client_worker = ClientWorker::connect().await?;

    info!("Running worker indefinitely.");
    client_worker.run_indefinitely().await?;

    Ok(())
}
