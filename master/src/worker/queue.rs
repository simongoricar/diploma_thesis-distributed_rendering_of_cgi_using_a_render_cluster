use miette::miette;
use miette::Result;
use shared::jobs::BlenderJob;

pub struct FrameOnWorker {
    pub job: BlenderJob,
    pub frame_index: usize,
}

impl FrameOnWorker {
    pub fn new(job: BlenderJob, frame_index: usize) -> Self {
        Self { job, frame_index }
    }
}

pub struct WorkerQueue {
    queue: Vec<FrameOnWorker>,
}

impl WorkerQueue {
    pub fn new() -> Self {
        Self { queue: Vec::new() }
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn add(&mut self, frame: FrameOnWorker) {
        self.queue.push(frame);
    }

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
