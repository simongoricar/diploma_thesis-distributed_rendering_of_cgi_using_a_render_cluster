use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_channel::mpsc::UnboundedSender;
use log::{error, info};
use shared::jobs::BlenderJob;
use shared::messages::queue::{FrameQueueRemoveResult, WorkerFrameQueueItemFinishedEvent};
use shared::messages::traits::IntoWebSocketMessage;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite;

use crate::rendering::runner::BlenderJobRunner;

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum WorkerFrameState {
    Queued,
    Rendering,
    #[allow(dead_code)]
    Finished,
}

#[derive(Clone, Eq, PartialEq)]
pub struct WorkerQueueFrame {
    pub job: BlenderJob,
    pub frame_index: usize,
    pub state: WorkerFrameState,
}

impl WorkerQueueFrame {
    pub fn new(job: BlenderJob, frame_index: usize) -> Self {
        Self {
            job,
            frame_index,
            state: WorkerFrameState::Queued,
        }
    }
}


pub struct WorkerAutomaticQueue {
    runner: Arc<BlenderJobRunner>,
    frames: Arc<Mutex<Vec<WorkerQueueFrame>>>,
    message_sender: Arc<UnboundedSender<tungstenite::Message>>,
}

impl WorkerAutomaticQueue {
    pub fn new(
        runner: BlenderJobRunner,
        message_sender: Arc<UnboundedSender<tungstenite::Message>>,
    ) -> Self {
        Self {
            runner: Arc::new(runner),
            frames: Arc::new(Mutex::new(Vec::new())),
            message_sender,
        }
    }

    pub async fn start(&self) {
        tokio::spawn(Self::run_automatic_queue(
            self.runner.clone(),
            self.frames.clone(),
            self.message_sender.clone(),
        ));
    }

    async fn run_automatic_queue(
        runner: Arc<BlenderJobRunner>,
        frames: Arc<Mutex<Vec<WorkerQueueFrame>>>,
        message_sender: Arc<UnboundedSender<tungstenite::Message>>,
    ) {
        let is_currently_running = Arc::new(AtomicBool::new(false));

        loop {
            tokio::time::sleep(Duration::from_millis(25)).await;

            if is_currently_running.load(Ordering::SeqCst) {
                continue;
            }

            let mut locked_frames = frames.lock().await;

            let pending_frame = locked_frames
                .iter_mut()
                .find(|frame| frame.state == WorkerFrameState::Queued);

            if let Some(frame) = pending_frame {
                if !is_currently_running.load(Ordering::SeqCst) {
                    info!(
                        "Spawning new frame renderer: {}, frame {}",
                        frame.job.job_name, frame.frame_index
                    );

                    is_currently_running.store(true, Ordering::SeqCst);
                    tokio::spawn(Self::render_frame_and_report_through_websocket(
                        runner.clone(),
                        message_sender.clone(),
                        is_currently_running.clone(),
                        frames.clone(),
                        frame.job.clone(),
                        frame.frame_index,
                    ));
                }
            }

            drop(locked_frames);
        }
    }

    async fn render_frame_and_report_through_websocket(
        runner: Arc<BlenderJobRunner>,
        message_sender: Arc<UnboundedSender<tungstenite::Message>>,
        running_flag: Arc<AtomicBool>,
        frames: Arc<Mutex<Vec<WorkerQueueFrame>>>,
        job: BlenderJob,
        frame_index: usize,
    ) {
        running_flag.store(true, Ordering::SeqCst);

        let job_name = job.job_name.clone();

        {
            let mut frames_locked = frames.lock().await;

            let our_frame = frames_locked
                .iter_mut()
                .find(|frame| frame.job == job && frame.frame_index == frame_index)
                .expect("BUG: No such frame.");

            our_frame.state = WorkerFrameState::Rendering;
        }

        let render_result = runner.render_frame(job.clone(), frame_index).await;
        match render_result {
            Ok(_) => {
                info!(
                    "Frame has been successfully rendered: {}, frame {}.",
                    job_name, frame_index
                );

                // Report back to master that we finished this frame.
                let send_result = WorkerFrameQueueItemFinishedEvent::new_ok(job_name, frame_index)
                    .into_ws_message()
                    .send(&message_sender);

                if let Err(error) = send_result {
                    error!(
                        "Errored while sending item finished notification: {}",
                        error
                    );
                };
            }
            Err(error) => {
                error!("Errored while rendering frame: {}", error);
            }
        }

        {
            let mut frames_locked = frames.lock().await;

            let our_frame_index = frames_locked
                .iter_mut()
                .position(|frame| frame.job == job && frame.frame_index == frame_index)
                .expect("BUG: No such frame.");
            frames_locked.remove(our_frame_index);
        }

        running_flag.store(false, Ordering::SeqCst);
    }

    pub async fn queue_frame(&self, job: BlenderJob, frame_index: usize) {
        let frame = WorkerQueueFrame::new(job, frame_index);

        {
            let mut locked_frames_vec = self.frames.lock().await;

            locked_frames_vec.push(frame);
        }
    }

    pub async fn unqueue_frame(
        &self,
        job_name: String,
        frame_index: usize,
    ) -> FrameQueueRemoveResult {
        let mut locked_frames_vec = self.frames.lock().await;

        let frame = locked_frames_vec
            .iter()
            .enumerate()
            .find(|(_, frame)| frame.job.job_name == job_name && frame.frame_index == frame_index)
            .ok_or_else(|| FrameQueueRemoveResult::Errored {
                reason: "Can't find such queued frame.".to_string(),
            });

        let (frame_array_index, frame) = match frame {
            Ok(frame) => frame,
            Err(result) => {
                return result;
            }
        };

        if frame.state == WorkerFrameState::Rendering {
            FrameQueueRemoveResult::AlreadyRendering
        } else if frame.state == WorkerFrameState::Finished {
            FrameQueueRemoveResult::AlreadyFinished
        } else {
            locked_frames_vec.remove(frame_array_index);

            FrameQueueRemoveResult::RemovedFromQueue
        }
    }
}
