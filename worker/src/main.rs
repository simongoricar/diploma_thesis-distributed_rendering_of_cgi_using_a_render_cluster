mod cli;
mod connection;
mod rendering;
mod utilities;

use std::io;
use std::io::Write;
use std::path::PathBuf;

use clap::Parser;
use miette::{miette, Context, Result};
use shared::results::worker_trace::WorkerTraceBuilder;
use tracing::info;
use tracing_appender::rolling::RollingFileAppender;
use tracing_subscriber::EnvFilter;

use crate::cli::CLIArgs;
use crate::connection::Worker;
use crate::rendering::runner::BlenderJobRunner;
use crate::utilities::parse_with_tilde_support;


enum LogFileOutputMode {
    None,
    Some {
        directory_path: PathBuf,
        log_file_name: String,
    },
}

enum FileOutputWriter {
    NoOutput,
    Some(RollingFileAppender),
}

impl Write for FileOutputWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            FileOutputWriter::Some(writer) => writer.write(buf),
            _ => Ok(buf.len()),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            FileOutputWriter::Some(writer) => writer.flush(),
            _ => Ok(()),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = CLIArgs::parse();

    let log_output_mode = if let Some(output_path) = &args.log_output_file_path {
        let directory = output_path
            .parent()
            .ok_or_else(|| miette!("Could not parse --logFilePath's parent directory path."))?;

        let file_name = output_path
            .file_name()
            .ok_or_else(|| miette!("Could not parse --logFilePath's file name."))?
            .to_string_lossy()
            .to_string();

        LogFileOutputMode::Some {
            directory_path: directory.to_path_buf(),
            log_file_name: file_name,
        }
    } else {
        LogFileOutputMode::None
    };

    let (log_file_writer, _guard) = tracing_appender::non_blocking(
        if let LogFileOutputMode::Some {
            directory_path,
            log_file_name,
        } = log_output_mode
        {
            FileOutputWriter::Some(tracing_appender::rolling::never(
                directory_path,
                log_file_name,
            ))
        } else {
            FileOutputWriter::NoOutput
        },
    );

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(log_file_writer)
        .init();


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
