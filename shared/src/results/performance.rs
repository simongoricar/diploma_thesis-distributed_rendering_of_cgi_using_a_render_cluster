use std::time::Duration;

use miette::{miette, Context, IntoDiagnostic, Result};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use serde_with::DurationSecondsWithFrac;

use crate::results::worker_trace::WorkerTrace;

#[serde_as]
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct WorkerPerformance {
    /// Total amount of frames rendered on this worker.
    pub total_frames_rendered: usize,

    /// Total amount of frames ever queued during this job on this worker.
    pub total_frames_queued: usize,

    /// Total amount of frames ever un-queued during this job on this worker.
    pub total_frames_stolen_from_queue: usize,

    /// Total worker run time.
    #[serde_as(as = "DurationSecondsWithFrac<f64>")]
    pub total_time: Duration,

    /// Total time spent reading the `.blend` project files on this worker.
    #[serde_as(as = "DurationSecondsWithFrac<f64>")]
    pub total_blend_file_reading_time: Duration,

    /// Total time spent rendering frames on this worker.
    #[serde_as(as = "DurationSecondsWithFrac<f64>")]
    pub total_rendering_time: Duration,

    /// Total time spent saving rendered frames on this worker.
    #[serde_as(as = "DurationSecondsWithFrac<f64>")]
    pub total_image_saving_time: Duration,

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
            .signed_duration_since(trace.job_start_time)
            .to_std()
            .into_diagnostic()
            .wrap_err_with(|| miette!("Could not calculate total job duration."))?;

        let mut total_blend_file_reading_time = Duration::new(0, 0);
        let mut total_rendering_time = Duration::new(0, 0);
        let mut total_image_saving_time = Duration::new(0, 0);
        let mut total_idle_time = Duration::new(0, 0);


        let total_frames = trace.frame_render_traces.len();

        for frame_index in 0..total_frames {
            let current_frame = &trace.frame_render_traces[frame_index].details;

            let blend_file_reading_duration = current_frame
                .finished_loading_at
                .signed_duration_since(current_frame.started_process_at)
                .to_std()
                .into_diagnostic()
                .wrap_err_with(|| miette!("Invalid file reading duration."))?;
            total_blend_file_reading_time += blend_file_reading_duration;

            let rendering_duration = current_frame
                .finished_rendering_at
                .signed_duration_since(current_frame.started_rendering_at)
                .to_std()
                .into_diagnostic()
                .wrap_err_with(|| miette!("Invalid rendering duration."))?;
            total_rendering_time += rendering_duration;

            let image_saving_duration = current_frame
                .file_saving_finished_at
                .signed_duration_since(current_frame.file_saving_started_at)
                .to_std()
                .into_diagnostic()
                .wrap_err_with(|| miette!("Invalid file saving duration."))?;
            total_image_saving_time += image_saving_duration;


            // TODO Rewrite this parsing with the new detailed tracing system.
            if frame_index == 0 {
                // This is the first frame (no previous frame to subtract with).
                let idle_time_before_first_frame = current_frame
                    .started_process_at
                    .signed_duration_since(trace.job_start_time)
                    .to_std()
                    .into_diagnostic()
                    .wrap_err_with(|| {
                        miette!("Failed to calculate idle time before first frame.")
                    })?;

                total_idle_time += idle_time_before_first_frame;
            } else if frame_index == total_frames - 1 {
                let idle_time_after_last_frame = trace
                    .job_finish_time
                    .signed_duration_since(current_frame.exited_process_at)
                    .to_std()
                    .into_diagnostic()
                    .wrap_err_with(|| miette!("Failed to calculate idle time after last frame."))?;

                total_idle_time += idle_time_after_last_frame;
            } else {
                let previous_frame = &trace.frame_render_traces[frame_index - 1].details;
                let idle_time_between_frames = current_frame
                    .started_process_at
                    .signed_duration_since(previous_frame.exited_process_at)
                    .to_std()
                    .into_diagnostic()
                    .wrap_err_with(|| miette!("Invalid idle duration."))?;

                total_idle_time += idle_time_between_frames;
            }
        }

        Ok(Self {
            total_frames_rendered,
            total_frames_queued,
            total_frames_stolen_from_queue,
            total_time,
            total_blend_file_reading_time,
            total_rendering_time,
            total_image_saving_time,
            total_idle_time,
        })
    }
}
