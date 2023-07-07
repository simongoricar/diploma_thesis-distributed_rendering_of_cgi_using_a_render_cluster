pub mod cli;
pub mod cluster;
pub mod connection;

use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use chrono::{DateTime, Local};
use clap::Parser;
use miette::{miette, Context, IntoDiagnostic, Result};
use serde::Serialize;
use shared::jobs::BlenderJob;
use shared::logging::initialize_console_and_file_logging;
use shared::results::master_trace::MasterTrace;
use shared::results::performance::WorkerPerformance;
use shared::results::worker_trace::WorkerTrace;
use tracing::info;

use crate::cli::{CLIArgs, CLICommand};
use crate::cluster::ClusterManager;

fn parse_worker_traces(
    worker_traces: Vec<(SocketAddr, WorkerTrace)>,
) -> Result<Vec<(SocketAddr, WorkerPerformance)>> {
    worker_traces
        .into_iter()
        .map(|(address, trace)| {
            let performance = WorkerPerformance::from_worker_trace(&trace);

            match performance {
                Ok(perf) => Ok((address, perf)),
                Err(error) => Err(error),
            }
        })
        .collect::<Result<Vec<(SocketAddr, WorkerPerformance)>>>()
}

#[derive(Serialize)]
struct RawTraceWrapper {
    pub master_trace: MasterTrace,
    pub worker_traces: HashMap<String, WorkerTrace>,
}

fn save_raw_traces(
    start_time: &DateTime<Local>,
    job: &BlenderJob,
    output_directory: &Path,
    master_trace: &MasterTrace,
    worker_traces: &[(SocketAddr, WorkerTrace)],
) -> Result<()> {
    let traces_hash_map = worker_traces
        .iter()
        .map(|(address, trace)| {
            (
                // TODO Add hostname prefix (or maybe a worker ID?)
                format!("{}:{}", address.ip(), address.port()),
                trace.clone(),
            )
        })
        .collect();

    let wrapped_raw_traces = RawTraceWrapper {
        master_trace: master_trace.clone(),
        worker_traces: traces_hash_map,
    };

    let serialized_traces = serde_json::to_string_pretty(&wrapped_raw_traces)
        .into_diagnostic()
        .wrap_err_with(|| miette!("Failed to serialize raw traces."))?;


    let raw_traces_file_name = format!(
        "{}_job-{}_raw-trace.json",
        start_time.format("%Y-%m-%d_%H-%M-%S"),
        job.job_name.replace(' ', "_"),
    );
    let raw_traces_full_path = output_directory.join(raw_traces_file_name);


    if !output_directory.is_dir() {
        fs::create_dir_all(output_directory)
            .into_diagnostic()
            .wrap_err_with(|| miette!("Failed to create output directory."))?;
    }

    let mut output_file = File::create(raw_traces_full_path)
        .into_diagnostic()
        .wrap_err_with(|| miette!("Failed to create raw traces file."))?;

    output_file
        .write_all(serialized_traces.as_bytes())
        .into_diagnostic()
        .wrap_err_with(|| miette!("Failed to write raw traces to output file."))?;

    Ok(())
}

#[derive(Serialize)]
struct ProcessedResultsWrapper {
    pub worker_performance: HashMap<String, WorkerPerformance>,
}

fn save_processed_results(
    start_time: &DateTime<Local>,
    job: &BlenderJob,
    output_directory: &Path,
    worker_performance: &[(SocketAddr, WorkerPerformance)],
) -> Result<()> {
    let worker_perf_map: HashMap<String, WorkerPerformance> = worker_performance
        .iter()
        .map(|(address, performance)| {
            (
                format!("{}:{}", address.ip(), address.port()),
                performance.clone(),
            )
        })
        .collect();

    let wrapped_results = ProcessedResultsWrapper {
        worker_performance: worker_perf_map,
    };

    let serialized_results = serde_json::to_string_pretty(&wrapped_results)
        .into_diagnostic()
        .wrap_err_with(|| miette!("Failed to serialize processed results."))?;


    let processed_results_file_name = format!(
        "{}_job-{}_processed-results.json",
        start_time.format("%Y-%m-%d_%H-%M-%S"),
        job.job_name.replace(' ', "_"),
    );
    let processed_results_full_path = output_directory.join(processed_results_file_name);

    if !output_directory.is_dir() {
        fs::create_dir_all(output_directory)
            .into_diagnostic()
            .wrap_err_with(|| miette!("Failed to create output directory."))?;
    }

    let mut output_file = File::create(processed_results_full_path)
        .into_diagnostic()
        .wrap_err_with(|| miette!("Failed to create processed results file."))?;

    output_file
        .write_all(serialized_results.as_bytes())
        .into_diagnostic()
        .wrap_err_with(|| miette!("Failed to write processed results to output file."))?;

    Ok(())
}

fn print_results(
    master_performance: &MasterTrace,
    worker_performance: &Vec<(SocketAddr, WorkerPerformance)>,
) -> Result<()> {
    /*
     * Individual worker statistics
     */
    println!();
    println!("Worker performance results:");
    println!();

    let mut cumulative_frames_rendered: usize = 0;
    let mut cumulative_frames_queued: usize = 0;
    let mut cumulative_frames_stolen: usize = 0;

    let mut cumulative_blend_file_reading_time = Duration::new(0, 0);
    let mut cumulative_rendering_time = Duration::new(0, 0);
    let mut cumulative_image_saving_time = Duration::new(0, 0);
    let mut cumulative_idle_time = Duration::new(0, 0);


    for (address, performance) in worker_performance {
        cumulative_frames_rendered += performance.total_frames_rendered;
        cumulative_frames_queued += performance.total_frames_queued;
        cumulative_frames_stolen += performance.total_frames_stolen_from_queue;

        cumulative_blend_file_reading_time += performance.total_blend_file_reading_time;
        cumulative_rendering_time += performance.total_rendering_time;
        cumulative_image_saving_time += performance.total_image_saving_time;
        cumulative_idle_time += performance.total_idle_time;



        println!("[Worker {}:{}]", address.ip(), address.port());

        println!(
            "Total queued frames = {}",
            performance.total_frames_queued
        );
        println!(
            "Total frames rendered = {}",
            performance.total_frames_rendered
        );
        println!(
            "Total frames stolen from worker's queue = {}",
            performance.total_frames_stolen_from_queue
        );

        println!(
            "On-job time = {:.6} seconds.",
            performance.total_time.as_secs_f64()
        );
        println!(
            ".blend file reading time = {:.6} seconds.",
            performance.total_blend_file_reading_time.as_secs_f64()
        );
        println!(
            "Rendering time = {:.6} seconds.",
            performance.total_rendering_time.as_secs_f64()
        );
        println!(
            "Image saving time = {:.6} seconds.",
            performance.total_image_saving_time.as_secs_f64()
        );
        println!(
            "Idle time = {:.6} seconds.",
            performance.total_idle_time.as_secs_f64()
        );

        println!();
    }

    /*
     * Cumulative worker statistics
     */
    println!("[Cumulative]");

    println!(
        "Cumulative frames rendered = {}",
        cumulative_frames_rendered
    );
    println!(
        "Cumulative frames added to queue = {}",
        cumulative_frames_queued
    );
    println!(
        "Cumulative frames stolen from workers' queues = {}",
        cumulative_frames_stolen
    );

    println!(
        "Cumulative .blend file reading time = {:.6} seconds.",
        cumulative_blend_file_reading_time.as_secs_f64()
    );
    println!(
        "Cumulative rendering time = {:.6} seconds.",
        cumulative_rendering_time.as_secs_f64()
    );
    println!(
        "Cumulative image saving time = {:.6} seconds.",
        cumulative_image_saving_time.as_secs_f64()
    );
    println!(
        "Cumulative idle time = {:.6} seconds.",
        cumulative_idle_time.as_secs_f64()
    );

    println!();

    /*
     * Master statistics
     */
    println!("[Master]");

    println!(
        "Total job duration = {:.6} seconds.",
        master_performance
            .job_finish_time
            .duration_since(master_performance.job_start_time)
            .into_diagnostic()?
            .as_secs_f64()
    );

    Ok(())
}


#[tokio::main]
async fn main() -> Result<()> {
    let args = CLIArgs::parse();

    let _guard = initialize_console_and_file_logging(args.log_output_file_path.as_ref())?;

    #[allow(irrefutable_let_patterns)]
    if let CLICommand::RunJob(run_job_args) = args.command {
        let start_time = Local::now();

        info!(
            job_file_path = run_job_args.job_file_path,
            "Loading job file.",
        );

        let job = BlenderJob::load_from_file(run_job_args.job_file_path)
            .wrap_err_with(|| miette!("Could not load Blender job from file."))?;


        info!("Initializing server and running until job is complete.");

        let (master_trace, worker_traces) = ClusterManager::initialize_server_and_run_job(
            &args.bind_to_host,
            args.bind_to_port,
            job.clone(),
        )
        .await
        .wrap_err_with(|| miette!("Failed to run master server to job completion."))?;

        info!(
            results_directory_path = run_job_args
                .results_directory_path
                .to_string_lossy()
                .to_string(),
            "Job has been completed and traces have been received, analyzing and saving.",
        );


        // Parse and save the results.
        info!("Saving raw traces.");
        save_raw_traces(
            &start_time,
            &job,
            &run_job_args.results_directory_path,
            &master_trace,
            &worker_traces,
        )?;

        info!("Processing traces.");
        let worker_performances = parse_worker_traces(worker_traces)?;

        info!("Saving processed results.");
        save_processed_results(
            &start_time,
            &job,
            &run_job_args.results_directory_path,
            &worker_performances,
        )?;

        print_results(&master_trace, &worker_performances)?;
    };

    Ok(())
}
