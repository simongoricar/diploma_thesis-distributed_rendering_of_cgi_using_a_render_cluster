use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
pub struct CLIArgs {
    #[command(subcommand)]
    pub command: CLICommand,

    #[arg(long = "host", help = "Host to bind the server to.")]
    pub bind_to_host: String,

    #[arg(long = "port", help = "Port to bind the server to.")]
    pub bind_to_port: usize,

    #[arg(
        long = "logFilePath",
        help = "Log output file path. Logs everything that is printed to the console."
    )]
    pub log_output_file_path: Option<PathBuf>,
}

#[derive(Subcommand)]
pub enum CLICommand {
    #[command(name = "run-job", about = "Run the specified job to completion.")]
    RunJob(RunJob),
}

#[derive(Args, Eq, PartialEq)]
pub struct RunJob {
    #[arg(help = "Path to the TOML file describing the job.")]
    pub job_file_path: String,

    #[arg(
        long = "resultsDirectory",
        help = "Directory to save the cluster results in."
    )]
    pub results_directory_path: PathBuf,
}
