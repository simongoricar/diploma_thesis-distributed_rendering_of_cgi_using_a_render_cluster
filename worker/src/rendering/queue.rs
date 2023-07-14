use std::sync::Arc;
use std::time::Duration;

use miette::{miette, Context};
use miette::{IntoDiagnostic, Result};
use shared::cancellation::CancellationToken;
use shared::jobs::BlenderJob;
use shared::messages::queue::{FrameQueueRemoveResult, WorkerFrameQueueItemFinishedEvent};
use shared::messages::SenderHandle;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{debug, error, info};

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
    frames: Arc<Mutex<Vec<WorkerQueueFrame>>>,

    join_handle: JoinHandle<Result<()>>,
}

impl WorkerAutomaticQueue {
    pub fn initialize(
        runner: BlenderJobRunner,
        sender_handle: SenderHandle,
        global_cancellation_token: CancellationToken,
    ) -> Self {
        let runner_arc = Arc::new(runner);
        let frames_arc = Arc::new(Mutex::new(Vec::new()));

        let join_handle = tokio::spawn(Self::run_automatic_queue(
            runner_arc,
            frames_arc.clone(),
            sender_handle,
            global_cancellation_token,
        ));

        Self {
            frames: frames_arc,
            join_handle,
        }
    }

    pub async fn join(self) -> Result<()> {
        self.join_handle.await.into_diagnostic()?
    }

    async fn run_automatic_queue(
        runner: Arc<BlenderJobRunner>,
        frames: Arc<Mutex<Vec<WorkerQueueFrame>>>,
        sender_handle: SenderHandle,
        global_cancellation_token: CancellationToken,
    ) -> Result<()> {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;

            if global_cancellation_token.is_cancelled() {
                debug!("Stopping automatic rendering queue (worker stopping).");
                break;
            }

            let frame = {
                let mut locked_frames = frames.lock().await;
                let pending_frame = locked_frames
                    .iter_mut()
                    .find(|frame| frame.state == WorkerFrameState::Queued);

                match pending_frame {
                    Some(frame) => frame.clone(),
                    None => {
                        continue;
                    }
                }
            };

            info!(
                job_name = frame.job.job_name,
                frame_index = frame.frame_index,
                "Starting to render new frame."
            );

            Self::render_frame_and_report_through_websocket(
                runner.clone(),
                sender_handle.clone(),
                frames.clone(),
                frame.job.clone(),
                frame.frame_index,
            )
            .await;
        }

        Ok(())
    }

    async fn render_frame_and_report_through_websocket(
        runner: Arc<BlenderJobRunner>,
        sender_handle: SenderHandle,
        frames: Arc<Mutex<Vec<WorkerQueueFrame>>>,
        job: BlenderJob,
        frame_index: usize,
    ) {
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
                // Report back to master that we finished this frame.
                let send_result = sender_handle
                    .send_message(WorkerFrameQueueItemFinishedEvent::new_ok(
                        job_name.clone(),
                        frame_index,
                    ))
                    .await
                    .wrap_err_with(|| miette!("Failed to send frame finished event."));

                if let Err(error) = send_result {
                    error!(
                        error = ?error,
                        job_name = job_name,
                        frame_index = frame_index,
                        "Frame has been successfully rendered, but we failed to report that to the master server!"
                    );
                } else {
                    info!(
                        job_name = job_name,
                        frame_index = frame_index,
                        "Frame has been successfully rendered and progress has been reported to the master server."
                    );
                }
            }
            Err(error) => {
                error!(
                    error = ?error,
                    "Frame failed to render!"
                );
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
