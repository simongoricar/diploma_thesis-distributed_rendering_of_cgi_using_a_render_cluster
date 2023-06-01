use std::collections::HashMap;
use std::net::SocketAddr;

use miette::miette;
use miette::Result;
use shared::jobs::BlenderJob;
use tokio::sync::Mutex;

use crate::worker::Worker;

#[derive(Eq, PartialEq, Copy, Clone)]
pub enum FrameStatus {
    Pending,
    QueuedOnWorker { worker: SocketAddr },
    Finished,
}

pub struct BlenderJobFrame {
    pub job: BlenderJob,
    pub frame_index: usize,
    pub status: FrameStatus,
}

impl BlenderJobFrame {
    pub fn new_pending(job: BlenderJob, frame_index: usize) -> Self {
        Self {
            job,
            frame_index,
            status: FrameStatus::Pending,
        }
    }
}


pub struct ClusterManagerState {
    pub workers: Mutex<HashMap<SocketAddr, Worker>>,
    pub job_frames: Mutex<Vec<BlenderJobFrame>>,
}

impl ClusterManagerState {
    pub fn new_from_job(job: BlenderJob) -> Self {
        let pending_job_frames: Vec<BlenderJobFrame> = (job.frame_range_from..=job.frame_range_to)
            .map(|frame_index| BlenderJobFrame::new_pending(job.clone(), frame_index))
            .collect();

        Self {
            workers: Mutex::new(HashMap::with_capacity(
                job.wait_for_number_of_workers,
            )),
            job_frames: Mutex::new(pending_job_frames),
        }
    }

    pub async fn next_pending_frame(&self) -> Option<usize> {
        self.job_frames
            .lock()
            .await
            .iter()
            .find(|frame| frame.status == FrameStatus::Pending)
            .map(|frame| frame.frame_index)
    }

    pub async fn all_frames_finished(&self) -> bool {
        for frame in self.job_frames.lock().await.iter() {
            if frame.status != FrameStatus::Finished {
                return false;
            }
        }

        return true;
    }

    pub async fn mark_frame_as_queued_on_worker(
        &self,
        worker_address: SocketAddr,
        frame_index: usize,
    ) -> Result<()> {
        let mut locked_frames = self.job_frames.lock().await;

        let frame = locked_frames
            .iter_mut()
            .find(|frame| frame.frame_index == frame_index)
            .ok_or_else(|| miette!("Could not find frame with given index."))?;

        frame.status = FrameStatus::QueuedOnWorker {
            worker: worker_address,
        };
        Ok(())
    }

    pub async fn mark_frame_as_finished(&self, frame_index: usize) -> Result<()> {
        let mut locked_frames = self.job_frames.lock().await;

        let frame = locked_frames
            .iter_mut()
            .find(|frame| frame.frame_index == frame_index)
            .ok_or_else(|| miette!("Could not find frame with given index."))?;

        frame.status = FrameStatus::Finished;
        Ok(())
    }
}
