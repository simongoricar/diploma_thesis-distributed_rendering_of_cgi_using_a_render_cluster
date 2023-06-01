use miette::miette;
use miette::Result;
use shared::jobs::BlenderJob;

/// Represents a single queue item on the worker.
pub struct FrameOnWorker {
    pub job: BlenderJob,
    pub frame_index: usize,
}

impl FrameOnWorker {
    pub fn new(job: BlenderJob, frame_index: usize) -> Self {
        Self { job, frame_index }
    }
}

/// Master server's replica of the worker queue.
/// Can get out of sync with the actual worker, but unless something goes horribly wrong, not for long.
pub struct WorkerQueue {
    queue: Vec<FrameOnWorker>,
}

impl WorkerQueue {
    /// Initialize a new `WorkerQueue`.
    pub fn new() -> Self {
        Self { queue: Vec::new() }
    }

    /// Returns `true` if the worker queue is empty.
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Add a new frame to the worker's queue.
    pub fn add(&mut self, frame: FrameOnWorker) {
        self.queue.push(frame);
    }

    /// Remove a frame from the worker's queue.
    pub fn remove(&mut self, job_name: String, frame_index: usize) -> Result<()> {
        let item_index = self
            .queue
            .iter()
            .position(|item| item.job.job_name == job_name && item.frame_index == frame_index)
            .ok_or_else(|| miette!("No such frame in queue."))?;

        self.queue.remove(item_index);
        Ok(())
    }
}
