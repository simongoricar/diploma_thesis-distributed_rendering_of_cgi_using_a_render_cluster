use std::sync::Arc;
use std::time::SystemTime;

use miette::{miette, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

#[derive(Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize, Debug)]
pub struct WorkerFrameTrace {
    pub frame_index: usize,
    pub frame_start_time: SystemTime,
    pub frame_finish_time: SystemTime,
}

impl WorkerFrameTrace {
    pub fn new(frame_index: usize, start_time: SystemTime, finish_time: SystemTime) -> Self {
        Self {
            frame_index,
            frame_start_time: start_time,
            frame_finish_time: finish_time,
        }
    }
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, Debug)]
pub struct WorkerTrace {
    /// Amount of frames added to the worker's queue by the master server.
    pub total_queued_frames: usize,

    /// Amount of frames removed from worker's queue by the master server.
    pub total_queued_frames_removed_from_queue: usize,

    pub job_start_time: SystemTime,

    pub job_finish_time: SystemTime,

    /// Information about all rendered frames (in the order they were rendered).
    pub frame_render_times: Vec<WorkerFrameTrace>,
}



struct WorkerTraceIncomplete {
    /// Amount of frames added to the worker's queue by the master server.
    pub total_queued_frames: usize,

    /// Amount of frames removed from worker's queue by the master server.
    pub total_queued_frames_removed_from_queue: usize,

    pub job_start_time: Option<SystemTime>,

    pub job_finish_time: Option<SystemTime>,

    /// Information about all rendered frames (in the order they were rendered).
    pub frame_render_times: Vec<WorkerFrameTrace>,
}

#[derive(Clone)]
pub struct WorkerTraceBuilder(Arc<Mutex<WorkerTraceIncomplete>>);

impl WorkerTraceBuilder {
    pub fn new_empty() -> Self {
        Self(Arc::new(Mutex::new(WorkerTraceIncomplete {
            total_queued_frames: 0,
            total_queued_frames_removed_from_queue: 0,
            job_start_time: None,
            job_finish_time: None,
            frame_render_times: Vec::new(),
        })))
    }

    pub async fn build(&self) -> Result<WorkerTrace> {
        let trace = &self.0.lock().await;

        Ok(WorkerTrace {
            total_queued_frames: trace.total_queued_frames,
            total_queued_frames_removed_from_queue: trace.total_queued_frames_removed_from_queue,
            job_start_time: trace
                .job_start_time
                .ok_or_else(|| miette!("Missing job start time, can't build."))?,
            job_finish_time: trace
                .job_finish_time
                .ok_or_else(|| miette!("Missing job finish time, can't build."))?,
            frame_render_times: trace.frame_render_times.clone(),
        })
    }


    pub async fn trace_new_frame_queued(&self) {
        self.0.lock().await.total_queued_frames += 1;
    }

    pub async fn trace_frame_stolen_from_queue(&self) {
        self.0.lock().await.total_queued_frames_removed_from_queue += 1;
    }

    pub async fn set_job_start_time(&self, start_time: SystemTime) {
        let _ = self.0.lock().await.job_start_time.insert(start_time);
    }

    pub async fn set_job_finish_time(&self, finish_time: SystemTime) {
        let _ = self.0.lock().await.job_finish_time.insert(finish_time);
    }

    pub async fn trace_new_rendered_frame(
        &self,
        frame_index: usize,
        start_time: SystemTime,
        finish_time: SystemTime,
    ) {
        self.0
            .lock()
            .await
            .frame_render_times
            .push(WorkerFrameTrace::new(
                frame_index,
                start_time,
                finish_time,
            ))
    }
}
