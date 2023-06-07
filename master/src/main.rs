pub mod cli;
pub mod cluster;
pub mod connection;

use std::net::SocketAddr;
use std::time::Duration;

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
        let (master_performance, worker_traces) = manager
            .run_job_to_completion()
            .await
            .wrap_err_with(|| miette!("Could not run server and job to completion."))?;
        info!("-- JOB COMPLETE, ANALYZING TRACES --");

        let worker_performance_traces = worker_traces
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

        let mut cumulative_rendering_time = Duration::new(0, 0);
        let mut cumulative_idle_time = Duration::new(0, 0);

        let mut cumulative_frames_queued: usize = 0;
        let mut cumulative_frames_rendered: usize = 0;
        let mut cumulative_frames_stolen: usize = 0;

        for (address, performance) in worker_performance_traces {
            cumulative_rendering_time += performance.total_rendering_time;
            cumulative_idle_time += performance.total_idle_time;

            cumulative_frames_queued += performance.total_frames_queued;
            cumulative_frames_rendered += performance.total_frames_rendered;
            cumulative_frames_stolen += performance.total_frames_stolen_from_queue;


            println!("[Worker {}:{}]", address.ip(), address.port());

            println!(
                "Worker on-job time = {:.6} seconds.",
                performance.total_time.as_secs_f64()
            );
            println!(
                "Worker rendering time = {:.6} seconds.",
                performance.total_rendering_time.as_secs_f64()
            );
            println!(
                "Worker idle time = {:.6} seconds.",
                performance.total_idle_time.as_secs_f64()
            );

            println!(
                "Worker queued frames = {}",
                performance.total_frames_queued
            );
            println!(
                "Worker frames rendered = {}",
                performance.total_frames_rendered
            );
            println!(
                "Worker frames stolen from its queue before rendered = {}",
                performance.total_frames_stolen_from_queue
            );

            println!();
        }


        println!("[Total]");

        // TODO Total on-job time?

        println!(
            "Cumulative rendering time = {:.6} seconds.",
            cumulative_rendering_time.as_secs_f64()
        );
        println!(
            "Cumulative idle time = {:.6} seconds.",
            cumulative_idle_time.as_secs_f64()
        );

        println!(
            "Cumulative queued frames = {}",
            cumulative_frames_queued
        );
        println!(
            "Cumulative frames rendered = {}",
            cumulative_frames_rendered
        );
        println!(
            "Cumulative frames stolen from workers' queues before rendered = {}",
            cumulative_frames_stolen
        );
    }

    Ok(())
}
