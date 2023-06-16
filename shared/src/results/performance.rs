use std::time::Duration;

use miette::{miette, Context, IntoDiagnostic, Result};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use serde_with::DurationSecondsWithFrac;

use crate::results::worker_trace::WorkerTrace;

#[serde_as]
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct MasterPerformance {
    #[serde_as(as = "DurationSecondsWithFrac<f64>")]
    pub total_time: Duration,
}

impl MasterPerformance {
    pub fn new(total_time: Duration) -> Self {
        Self { total_time }
    }
}


#[serde_as]
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct WorkerPerformance {
    pub total_frames_rendered: usize,

    pub total_frames_queued: usize,

    pub total_frames_stolen_from_queue: usize,

    /// Total worker run time.
    #[serde_as(as = "DurationSecondsWithFrac<f64>")]
    pub total_time: Duration,

    /// Total time spent rendering frames on this worker.
    #[serde_as(as = "DurationSecondsWithFrac<f64>")]
    pub total_rendering_time: Duration,

    /// Total time spent preparing or waiting for new frames on this worker.
    #[serde_as(as = "DurationSecondsWithFrac<f64>")]
    pub total_idle_time: Duration,
}

impl WorkerPerformance {
    pub fn from_worker_trace(trace: &WorkerTrace) -> Result<Self> {
        // Extract frame queue statistics.
        let total_frames_rendered = trace.frame_render_traces.len();
        let total_frames_queued = trace.total_queued_frames;
        let total_frames_stolen_from_queue = trace.total_queued_frames_removed_from_queue;

        // Extract time statistics.
        let total_time = trace
            .job_finish_time
            .duration_since(trace.job_start_time)
            .into_diagnostic()
            .wrap_err_with(|| miette!("Could not calculate total job time."))?;

        let mut total_rendering_time = Duration::new(0, 0);
        let mut total_idle_time = Duration::new(0, 0);

        let total_frames = trace.frame_render_traces.len();

        for frame_index in 0..total_frames {
            let current_frame = trace.frame_render_traces[frame_index];

            // Calculate idle time from beginning of job or previous frame.
            if frame_index > 0 {
                // First frame
                let previous_frame = trace.frame_render_traces[frame_index - 1];

                let idle_time_between_frames = current_frame
                    .frame_start_time
                    .duration_since(previous_frame.frame_finish_time)
                    .into_diagnostic()
                    .wrap_err_with(|| miette!("Could not calculate idle time between frames."))?;

                total_idle_time += idle_time_between_frames;
            } else if frame_index == total_frames - 1 {
                // Last frame
                let idle_time_until_job_completion = trace
                    .job_finish_time
                    .duration_since(current_frame.frame_finish_time)
                    .into_diagnostic()
                    .wrap_err_with(|| miette!("Could not calculate idle time after last frame."))?;

                total_idle_time += idle_time_until_job_completion;
            } else {
                let idle_time_before_first_frame = current_frame
                    .frame_start_time
                    .duration_since(trace.job_start_time)
                    .into_diagnostic()
                    .wrap_err_with(|| {
                        miette!("Could not calculate idle time before first frame.")
                    })?;

                total_idle_time += idle_time_before_first_frame;
            }

            // Calculate rendering time.
            let rendering_time = current_frame
                .frame_finish_time
                .duration_since(current_frame.frame_start_time)
                .into_diagnostic()
                .wrap_err_with(|| miette!("Could not calculate rendering time."))?;

            total_rendering_time += rendering_time;
        }

        Ok(Self {
            total_frames_rendered,
            total_frames_queued,
            total_frames_stolen_from_queue,
            total_time,
            total_rendering_time,
            total_idle_time,
        })
    }
}
