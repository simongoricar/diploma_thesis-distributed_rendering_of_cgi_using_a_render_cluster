use shared::jobs::BlenderJob;

pub struct FrameOnWorker {
    pub job: BlenderJob,
    pub frame_index: usize,
}

pub struct WorkerQueue {
    queue: Vec<FrameOnWorker>,
}

impl WorkerQueue {
    pub fn new() -> Self {
        Self {
            queue: Vec::new(),
        }
    }
}
