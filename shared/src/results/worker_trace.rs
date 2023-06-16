use std::sync::Arc;
use std::time::SystemTime;

use miette::{miette, Result};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use serde_with::TimestampSecondsWithFrac;
use tokio::sync::Mutex;

#[serde_as]
#[derive(Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize, Debug)]
pub struct WorkerFrameTrace {
    pub frame_index: usize,

    #[serde_as(as = "TimestampSecondsWithFrac<f64>")]
    pub frame_start_time: SystemTime,

    #[serde_as(as = "TimestampSecondsWithFrac<f64>")]
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

#[serde_as]
#[derive(Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize, Debug)]
pub struct WorkerPingTrace {
    #[serde_as(as = "TimestampSecondsWithFrac<f64>")]
    pub pinged_at: SystemTime,

    #[serde_as(as = "TimestampSecondsWithFrac<f64>")]
    pub received_at: SystemTime,
}

impl WorkerPingTrace {
    pub fn new(pinged_at: SystemTime, received_at: SystemTime) -> Self {
        Self {
            pinged_at,
            received_at,
        }
    }
}


#[serde_as]
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, Debug)]
pub struct WorkerTrace {
    /// Amount of frames added to the worker's queue by the master server.
    pub total_queued_frames: usize,

    /// Amount of frames removed from worker's queue by the master server.
    pub total_queued_frames_removed_from_queue: usize,

    /// Job start time as perceived by the worker.
    #[serde_as(as = "TimestampSecondsWithFrac<f64>")]
    pub job_start_time: SystemTime,

    /// Job finish time as perceived by the worker.
    #[serde_as(as = "TimestampSecondsWithFrac<f64>")]
    pub job_finish_time: SystemTime,

    /// Information about all rendered frames (in the order they were rendered).
    pub frame_render_traces: Vec<WorkerFrameTrace>,

    pub ping_traces: Vec<WorkerPingTrace>,
}



struct WorkerTraceIncomplete {
    /// Amount of frames added to the worker's queue by the master server.
    pub total_queued_frames: usize,

    /// Amount of frames removed from worker's queue by the master server.
    pub total_queued_frames_removed_from_queue: usize,

    pub job_start_time: Option<SystemTime>,

    pub job_finish_time: Option<SystemTime>,

    /// Information about all rendered frames (in the order they were rendered).
    pub frame_render_traces: Vec<WorkerFrameTrace>,

    pub ping_traces: Vec<WorkerPingTrace>,
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
            frame_render_traces: Vec::new(),
            ping_traces: Vec::new(),
        })))
    }

    pub async fn build(&self) -> Result<WorkerTrace> {
        let trace = self.0.lock().await;

        Ok(WorkerTrace {
            total_queued_frames: trace.total_queued_frames,
            total_queued_frames_removed_from_queue: trace.total_queued_frames_removed_from_queue,
            job_start_time: trace
                .job_start_time
                .ok_or_else(|| miette!("Missing job start time, can't build."))?,
            job_finish_time: trace
                .job_finish_time
                .ok_or_else(|| miette!("Missing job finish time, can't build."))?,
            frame_render_traces: trace.frame_render_traces.clone(),
            ping_traces: trace.ping_traces.clone(),
        })
    }


    pub async fn trace_new_frame_queued(&self) {
        let mut trace = self.0.lock().await;
        trace.total_queued_frames += 1;
    }

    pub async fn trace_frame_stolen_from_queue(&self) {
        let mut trace = self.0.lock().await;
        trace.total_queued_frames_removed_from_queue += 1;
    }

    pub async fn set_job_start_time(&self, start_time: SystemTime) {
        let mut trace = self.0.lock().await;
        trace.job_start_time = Some(start_time);
    }

    pub async fn set_job_finish_time(&self, finish_time: SystemTime) {
        let mut trace = self.0.lock().await;
        trace.job_finish_time = Some(finish_time);
    }

    pub async fn trace_new_rendered_frame(
        &self,
        frame_index: usize,
        start_time: SystemTime,
        finish_time: SystemTime,
    ) {
        let mut trace = self.0.lock().await;

        trace.frame_render_traces.push(WorkerFrameTrace::new(
            frame_index,
            start_time,
            finish_time,
        ));
    }

    // TODO integrate
    pub async fn trace_new_ping(&self, pinged_at: SystemTime, received_at: SystemTime) {
        let mut trace = self.0.lock().await;

        trace
            .ping_traces
            .push(WorkerPingTrace::new(pinged_at, received_at));
    }
}
