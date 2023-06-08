use miette::miette;
use miette::Result;
use shared::jobs::BlenderJob;

#[derive(Eq, PartialEq, Clone, Copy, Debug)]
pub enum FrameStatusOnWorker {
    Queued,
    Rendering,
}

/// Represents a single queue item on the worker.
pub struct FrameOnWorker {
    pub job: BlenderJob,

    pub frame_index: usize,

    pub status: FrameStatusOnWorker,
}

impl FrameOnWorker {
    pub fn new_queued(job: BlenderJob, frame_index: usize) -> Self {
        Self {
            job,
            frame_index,
            status: FrameStatusOnWorker::Queued,
        }
    }

    pub fn set_rendering(&mut self) {
        self.status = FrameStatusOnWorker::Rendering;
    }
}

/// Master server's replica of the worker queue.
/// Can get out of sync with the actual worker, but unless something goes horribly wrong, not for long.
pub struct WorkerQueue {
    queue: Vec<FrameOnWorker>,
}

impl WorkerQueue {
    /// Initialize a new `WorkerQueue`.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self { queue: Vec::new() }
    }

    /// Returns `true` if the worker queue is empty.
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn num_queued_frames(&self) -> usize {
        self.queue
            .iter()
            .filter(|frame| frame.status == FrameStatusOnWorker::Queued)
            .count()
    }

    pub fn is_currently_rendering(&self) -> bool {
        self.queue
            .iter()
            .filter(|frame| frame.status == FrameStatusOnWorker::Rendering)
            .count()
            > 0
    }

    /// Add a new frame to the worker's queue.
    pub fn add(&mut self, frame: FrameOnWorker) {
        self.queue.push(frame);
    }

    pub fn set_frame_rendering(&mut self, job_name: String, frame_index: usize) -> Result<()> {
        let item = self
            .queue
            .iter_mut()
            .find(|item| item.job.job_name == job_name && item.frame_index == frame_index)
            .ok_or_else(|| miette!("No such frame in queue."))?;

        item.set_rendering();
        Ok(())
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
