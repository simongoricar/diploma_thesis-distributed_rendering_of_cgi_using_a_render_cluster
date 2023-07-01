use std::fs::create_dir_all;
use std::ops::Sub;
use std::path::PathBuf;
use std::process::Stdio;
use std::str::FromStr;
use std::sync::OnceLock;
use std::time::Duration;

use chrono::{DateTime, LocalResult, TimeZone, Utc};
use log::{debug, info};
use miette::{miette, Context, IntoDiagnostic, Result};
use regex::Regex;
use serde::Deserialize;
use shared::jobs::BlenderJob;
use shared::results::worker_trace::{FrameRenderTime, WorkerTraceBuilder};
use tokio::process::Command;

use crate::utilities::parse_with_base_directory_prefix;


struct PartialRenderStatistics {
    pub loaded_at: DateTime<Utc>,
    pub started_rendering_at: DateTime<Utc>,
    pub finished_rendering_at: DateTime<Utc>,
    pub file_saving_started_at: DateTime<Utc>,
    pub file_saving_finished_at: DateTime<Utc>,
}

impl PartialRenderStatistics {
    pub fn with_process_information(
        &self,
        started_process_at: DateTime<Utc>,
        exited_process_at: DateTime<Utc>,
    ) -> FrameRenderTime {
        FrameRenderTime {
            started_process_at,
            finished_loading_at: self.loaded_at,
            started_rendering_at: self.started_rendering_at,
            finished_rendering_at: self.finished_rendering_at,
            file_saving_started_at: self.file_saving_started_at,
            file_saving_finished_at: self.file_saving_finished_at,
            exited_process_at,
        }
    }
}



#[derive(Deserialize, Debug)]
struct BlenderRawResultsOutput {
    pub project_loaded_at: f64,
    pub project_started_rendering_at: f64,
    pub project_finished_rendering_at: f64,
}


/// Parses strings of format `mm:ss.fraction` into a `Duration`.
/// Returns `Err` if the string cannot be parsed.
fn parse_blender_human_time_into_duration(human_duration: &str) -> Result<Duration> {
    let slices: Vec<&str> = human_duration.split(&[':']).collect();

    if slices.len() != 2 {
        return Err(miette!(
            "Invalid human time, not in 00:00.00 format: {}",
            human_duration
        ));
    }

    #[allow(clippy::get_first)]
    let minutes = slices
        .get(0)
        .expect("BUG: slices had three elements, but get(0) failed.");
    let seconds = slices
        .get(1)
        .expect("BUG: slices had three elements, but get(1) failed.");

    let minutes = f64::from_str(minutes).into_diagnostic().wrap_err_with(|| {
        miette!(
            "Failed to decode minutes string to usize: {}",
            minutes
        )
    })?;
    let seconds = f64::from_str(seconds).into_diagnostic().wrap_err_with(|| {
        miette!(
            "Failed to decode seconds string to usize: {}",
            seconds
        )
    })?;

    Ok(Duration::from_secs_f64(minutes * 60f64 + seconds))
}


fn f64_to_utc_datetime(timestamp: f64) -> Result<DateTime<Utc>> {
    let full_second_time = timestamp as i64;

    let sub_second_time: f64 = timestamp - timestamp.floor();
    let sub_seconds_as_nanoseconds: u32 = (sub_second_time * 1e+9) as u32;

    match Utc.timestamp_opt(full_second_time, sub_seconds_as_nanoseconds) {
        LocalResult::Single(timestamp) => Ok(timestamp),
        _ => Err(miette!("Invalid timestamp: {timestamp}")),
    }
}


static BLENDER_TIME_AND_SAVING_REGEX: OnceLock<Regex> = OnceLock::new();

/// Given Blender's `stdout` (with our `render-timing-script.py` attached),
/// this function extracts the relevant information (project load time, rendering time, saving time).
///
/// Returns `Err` if any field cannot be parsed or any required data cannot be found.
fn extract_render_information<S: AsRef<str>>(stdout_output: S) -> Result<PartialRenderStatistics> {
    // FIXME Find out why `Time... (Saving ...)` isn't being found.
    let blender_time_and_saving_regex = BLENDER_TIME_AND_SAVING_REGEX.get_or_init(|| {
        Regex::new(r"Time: (?P<total_time>\d+:\d+\.\d+) \(?P<saving_time>Saving: (\d+:\d+\.\d+)\)")
            .expect("Invalid regex, can't compile!")
    });

    let mut saving_time: Option<Duration> = None;
    let mut raw_script_results: Option<BlenderRawResultsOutput> = None;

    for line in stdout_output.as_ref().lines() {
        if line.starts_with(" Time:") {
            let captures = blender_time_and_saving_regex.captures(line);
            let Some(captures) = captures else {
                continue
            };

            if saving_time.is_some() {
                return Err(miette!(
                    "Invalid Blender output: \" Time... (Saving ...)\" line appears more than once."
                ));
            }

            let saving_time_match = captures
                .name("saving_time")
                .expect("BUG: No such capture group: saving_time.");
            let saving_duration = parse_blender_human_time_into_duration(saving_time_match.as_str())
                .wrap_err_with(|| {
                    miette!("Failed to parse \"Time ... (Saving ...)\" into duration.")
                })?;

            saving_time = Some(saving_duration);
        } else if line.starts_with("RESULTS=") {
            let results_json_string = line
                .strip_prefix("RESULTS=")
                .expect("BUG: starts_with indicated wrong prefix.");

            let raw_results: BlenderRawResultsOutput = serde_json::from_str(results_json_string)
                .into_diagnostic()
                .wrap_err_with(|| miette!("Failed to decode RESULTS={}", results_json_string))?;

            raw_script_results = Some(raw_results);
        }
    }

    if raw_script_results.is_none() || saving_time.is_none() {
        return Err(miette!(
            "Invalid output, missing data (raw_script_results={:?},saving_time={:?}).",
            raw_script_results,
            saving_time
        ));
    }

    let saving_time = saving_time.unwrap();
    let raw_script_results = raw_script_results.unwrap();

    let loaded_at = f64_to_utc_datetime(raw_script_results.project_loaded_at)
        .wrap_err_with(|| miette!("Failed to parse loaded_at as DateTime<Utc>."))?;
    let started_rendering_at = f64_to_utc_datetime(raw_script_results.project_started_rendering_at)
        .wrap_err_with(|| {
            miette!("Failed to parse project_started_rendering_at as DateTime<Utc>.")
        })?;

    // Subtract saving time from end of render time
    let real_finished_rendering_at = raw_script_results
        .project_finished_rendering_at
        .sub(saving_time.as_secs_f64());

    let finished_rendering_at = f64_to_utc_datetime(real_finished_rendering_at)
        .wrap_err_with(|| miette!("Failed to parse real_finished_rendering_at as DateTime<Utc>."))?;
    let file_saving_started_at = f64_to_utc_datetime(real_finished_rendering_at)
        .wrap_err_with(|| miette!("Failed to parse real_finished_rendering_at as DateTime<Utc>."))?;

    // See `real_finished_rendering_at`. The `project_finished_rendering_at` includes the saving time as well.
    let file_saving_finished_at = f64_to_utc_datetime(
        raw_script_results.project_finished_rendering_at,
    )
    .wrap_err_with(|| miette!("Failed to parse project_finished_rendering_at as DateTime<Utc>."))?;

    Ok(PartialRenderStatistics {
        loaded_at,
        started_rendering_at,
        finished_rendering_at,
        file_saving_started_at,
        file_saving_finished_at,
    })
}



pub struct BlenderJobRunner {
    blender_binary_path: PathBuf,

    blender_prepend_arguments: Vec<String>,

    blender_append_arguments: Vec<String>,

    base_directory_path: PathBuf,

    tracer: WorkerTraceBuilder,
}

impl BlenderJobRunner {
    pub fn new(
        blender_binary: PathBuf,
        blender_prepend_arguments: Option<String>,
        blender_append_arguments: Option<String>,
        base_directory_path: PathBuf,
        tracer: WorkerTraceBuilder,
    ) -> Result<Self> {
        if !blender_binary.is_file() {
            return Err(miette!("Provided Blender path is not a file."));
        }

        if !base_directory_path.is_dir() {
            return Err(miette!(
                "Provided base directory path is not a directory."
            ));
        }

        let parsed_prepend_arguments: Vec<String> = if let Some(prepend) = blender_prepend_arguments
        {
            shlex::split(&prepend).ok_or_else(|| miette!("Failed to parse prepend arguments."))?
        } else {
            Vec::new()
        };

        let parsed_append_arguments: Vec<String> = if let Some(append) = blender_append_arguments {
            shlex::split(&append).ok_or_else(|| miette!("Failed to parse append arguments."))?
        } else {
            Vec::new()
        };

        debug!(
            "Parsed prepend arguments: {:?}",
            parsed_prepend_arguments
        );

        Ok(Self {
            blender_binary_path: blender_binary,
            blender_prepend_arguments: parsed_prepend_arguments,
            blender_append_arguments: parsed_append_arguments,
            base_directory_path,
            tracer,
        })
    }

    pub async fn render_frame(&self, job: BlenderJob, frame_index: usize) -> Result<()> {
        /*
         * Parse path to .blend project file
         */
        let blender_file_path = parse_with_base_directory_prefix(
            &job.project_file_path,
            Some(&self.base_directory_path),
        )?;

        // Ensure blender project file exists.
        if !blender_file_path.is_file() {
            return Err(miette!(
                "Invalid blender project file path: file doesn't exist: {:?}",
                blender_file_path
            ));
        }

        let blender_file_path_str = blender_file_path.to_string_lossy().to_string();

        /*
         * Parse path to .py script file
         */
        let render_script_path = parse_with_base_directory_prefix(
            &job.render_script_path,
            Some(&self.base_directory_path),
        )?;

        if !render_script_path.is_file() {
            return Err(miette!(
                "Invalid render script: file doesn't exist: {:?}",
                blender_file_path
            ));
        }

        let render_script_path_str = render_script_path.to_string_lossy().to_string();

        /*
         * Parse output file
         */
        let output_file_path_str = {
            debug!(
                "Before parsing: output_directory_path is {}",
                job.output_directory_path
            );

            let output_directory = parse_with_base_directory_prefix(
                &job.output_directory_path,
                Some(&self.base_directory_path),
            )?;

            if !output_directory.is_dir() {
                create_dir_all(&output_directory)
                    .into_diagnostic()
                    .wrap_err_with(|| miette!("Could not create missing directories."))?;
            }


            let mut output_path = output_directory.to_string_lossy().to_string();
            debug!(
                "After parsing: output_directory_path is {}",
                output_path
            );

            output_path.push('/');
            output_path.push_str(&job.output_file_name_format);

            output_path
        };

        info!("Starting to render frame {}.", frame_index);

        let mut blender_args = self.blender_prepend_arguments.clone();
        blender_args.extend(
            [
                &blender_file_path_str,
                "--background",
                "--python",
                &render_script_path_str,
                "--",
                "--render-output",
                &output_file_path_str,
                "--render-format",
                &job.output_file_format,
                "--render-frame",
                &frame_index.to_string(),
            ]
            .into_iter()
            .map(String::from),
        );
        blender_args.extend(self.blender_append_arguments.iter().cloned());

        debug!("Blender arguments: {:?}", blender_args);

        // TODO Test new expanded tracing with `render-timing-script.py`

        let process_start_time = Utc::now();

        let output = Command::new(&self.blender_binary_path)
            .args(blender_args)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output()
            .await
            .into_diagnostic()
            .wrap_err_with(|| miette!("Failed while executing Blender binary."))?;

        let process_stop_time = Utc::now();

        /*
         * Parse output of render script and file writing time.
         */
        let stdout_output = String::from_utf8_lossy(&output.stdout);
        let blender_statistics = extract_render_information(stdout_output.as_ref())
            .wrap_err_with(|| "Failed to extract render information.")?;
        let blender_statistics =
            blender_statistics.with_process_information(process_start_time, process_stop_time);

        info!(
            "Rendered frame {} in {:.4} seconds.",
            frame_index,
            blender_statistics.total_execution_time()?.as_secs_f64()
        );

        self.tracer
            .trace_new_rendered_frame(frame_index, blender_statistics)
            .await;

        Ok(())
    }
}
