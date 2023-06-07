pub mod cli;
pub mod cluster;
pub mod connection;

use std::net::SocketAddr;

use clap::Parser;
use log::info;
use miette::{miette, Context, IntoDiagnostic, Result};
use shared::jobs::BlenderJob;
use shared::results::performance::WorkerPerformance;

use crate::cli::{CLIArgs, CLICommand};
use crate::cluster::ClusterManager;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::try_init().into_diagnostic()?;

    let args = CLIArgs::parse();

    #[allow(irrefutable_let_patterns)]
    if let CLICommand::RunJob(run_job_args) = args.command {
        info!("Loading job file.");
        let job = BlenderJob::load_from_file(run_job_args.job_file_path)
            .wrap_err_with(|| miette!("Could not load Blender job from file."))?;

        info!("Initializing cluster manager.");
        let manager = ClusterManager::new_from_job(args.bind_to_host, args.bind_to_port, job)
            .await
            .wrap_err_with(|| miette!("Could not initialize cluster manager."))?;

        info!("Running server to job completion.");
        let traces = manager
            .run_job_to_completion()
            .await
            .wrap_err_with(|| miette!("Could not run server and job to completion."))?;
        info!("-- JOB COMPLETE, ANALYZING TRACES --");

        let performance_traces = traces
            .into_iter()
            .map(|(address, trace)| {
                let performance = WorkerPerformance::from_worker_trace(&trace);

                match performance {
                    Ok(perf) => Ok((address, perf)),
                    Err(error) => Err(error),
                }
            })
            .collect::<Result<Vec<(SocketAddr, WorkerPerformance)>>>()?;

        println!("Worker performance results:");
        println!();

        for (address, performance) in performance_traces {
            println!("[Worker {}:{}]", address.ip(), address.port());

            println!(
                "Total active time = {:.6} seconds.",
                performance.total_time.as_secs_f64()
            );
            println!(
                "Total rendering time = {:.6} seconds.",
                performance.total_rendering_time.as_secs_f64()
            );
            println!(
                "Total idle time = {:.6} seconds.",
                performance.total_idle_time.as_secs_f64()
            );

            println!(
                "Total frames queued = {}",
                performance.total_frames_queued
            );
            println!(
                "Total frames rendered = {}",
                performance.total_frames_rendered
            );
            println!(
                "Total frames stolen from queue before rendered = {}",
                performance.total_frames_stolen_from_queue
            );
        }
    }

    Ok(())
}
