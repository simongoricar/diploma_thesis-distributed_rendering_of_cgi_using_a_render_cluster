use std::ops::Sub;
use std::str::FromStr;
use std::sync::OnceLock;
use std::time::Duration;

use chrono::{DateTime, LocalResult, TimeZone, Utc};
use log::warn;
use miette::{miette, Context, IntoDiagnostic, Result};
use regex::Regex;
use serde::Deserialize;
use shared::results::worker_trace::FrameRenderTime;


pub struct PartialRenderStatistics {
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
pub fn extract_blender_render_information<S: AsRef<str>>(
    stdout_output: S,
) -> Result<PartialRenderStatistics> {
    // FIXME Find out why `Time... (Saving ...)` isn't being found.
    let blender_time_and_saving_regex = BLENDER_TIME_AND_SAVING_REGEX.get_or_init(|| {
        Regex::new(r"Time: (?P<total_time>\d+:\d+\.\d+) \(Saving: (?P<saving_time>\d+:\d+\.\d+)\)")
            .expect("Invalid regex, can't compile!")
    });

    let mut saving_time: Option<Duration> = None;
    let mut raw_script_results: Option<BlenderRawResultsOutput> = None;

    // We skip the iterator until the `Saved: '<path>'` line happens. After that,
    // Blender should print the `Time: mm:ss.ss (Saving: mm:ss.ss)` line and after that, our
    // rendering script should print a line that starts with `RESULT=<some json>`.
    // None of those can appear before "Saved: ...", so we skip the iterator.
    let important_output_lines = stdout_output
        .as_ref()
        .lines()
        .skip_while(|line| !line.starts_with("Saved: '"));

    for line in important_output_lines {
        if line.starts_with(" Time:") {
            let captures = blender_time_and_saving_regex.captures(line);
            let Some(captures) = captures else {
                warn!("Line starts with \" Time:\", but no match.");
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
