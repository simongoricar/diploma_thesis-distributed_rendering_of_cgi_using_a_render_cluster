use miette::miette;
use miette::Result;
use shared::jobs::BlenderJob;

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum MasterFrameState {
    Pending,
    QueuedOnWorker,
    Finished,
}

#[derive(Clone)]
pub struct MasterFrame {
    index: usize,
    state: MasterFrameState,
}

impl MasterFrame {
    pub fn new_pending(index: usize) -> Self {
        Self::new(index, MasterFrameState::Pending)
    }

    pub fn new(index: usize, state: MasterFrameState) -> Self {
        Self { index, state }
    }
}


pub struct MasterBlenderJobState {
    job: BlenderJob,
    frames: Vec<MasterFrame>,
}

impl MasterBlenderJobState {
    pub fn from_job(job: BlenderJob) -> Self {
        let total_frames = job.frame_range_to - job.frame_range_from + 1;

        let states: Vec<MasterFrame> = (0..=total_frames)
            .map(|internal_index| {
                MasterFrame::new_pending(internal_index + job.frame_range_from)
            })
            .collect();

        Self {
            job,
            frames: states,
        }
    }

    pub fn next_frame_to_render(&self) -> Option<usize> {
        self.frames
            .iter()
            .find(|frame| frame.state == MasterFrameState::Pending)
            .map(|frame| frame.index)
    }

    pub fn have_all_frames_finished(&self) -> bool {
        for frame in self.frames.iter() {
            if frame.state != MasterFrameState::Finished {
                return false;
            }
        }

        true
    }

    pub fn mark_frame_as_queued_on_worker(
        &mut self,
        frame_index: usize,
    ) -> Result<()> {
        let internal_frame_index = frame_index - self.job.frame_range_from;

        let mut frame = self
            .frames
            .get_mut(internal_frame_index)
            .ok_or_else(|| miette!("Invalid frame_index!"))?;

        frame.state = MasterFrameState::QueuedOnWorker;
        Ok(())
    }

    pub fn set_frame_completed(&mut self, frame_index: usize) -> Result<()> {
        let frame = self
            .frames
            .get_mut(frame_index)
            .ok_or_else(|| miette!("frame_index out of bounds!"))?;

        frame.state = MasterFrameState::Finished;
        Ok(())
    }
}
