use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
pub struct CLIArgs {
    #[command(subcommand)]
    command: CLICommand,
}

#[derive(Subcommand)]
pub enum CLICommand {
    #[command(name = "run-job", about = "Run the specified job to completion.")]
    RunJob(RunJob),
}

#[derive(Args, Eq, PartialEq)]
pub struct RunJob {
    job_file_path: String,
}
