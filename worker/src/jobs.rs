use std::fs::create_dir_all;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_channel::mpsc::UnboundedSender;
use log::{error, info};
use miette::{miette, Context, IntoDiagnostic, Result};
use shared::jobs::BlenderJob;
use shared::messages::queue::WorkerFrameQueueItemFinishedNotification;
use shared::messages::WebSocketMessage;
use tokio::process::Command;
use tokio::sync::Mutex;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite;

pub fn parse_with_base_directory_prefix(
    path: &str,
    base: Option<&PathBuf>,
) -> Result<PathBuf> {
    if path.starts_with("%BASE%") {
        if base.is_none() {
            return Err(miette!("Missing base!"));
        }

        let prefixless_path =
            path.strip_prefix("%BASE%").expect("BUG: Missing prefix.");
        let prefixless_path =
            prefixless_path.strip_prefix('/').unwrap_or(prefixless_path);
        let prefixless_path = prefixless_path
            .strip_prefix('\\')
            .unwrap_or(prefixless_path);

        Ok(base.unwrap().join(prefixless_path))
    } else {
        Ok(PathBuf::from(path))
    }
}

pub struct BlenderJobRunner {
    blender_binary_path: PathBuf,
    base_directory_path: PathBuf,
}

impl BlenderJobRunner {
    pub fn new(
        blender_binary_path: PathBuf,
        base_directory_path: PathBuf,
    ) -> Result<Self> {
        if !blender_binary_path.is_file() {
            return Err(miette!("Provided Blender path is not a file."));
        }

        if !base_directory_path.is_dir() {
            return Err(miette!(
                "Provided base directory path is not a directory."
            ));
        }

        Ok(Self {
            blender_binary_path,
            base_directory_path,
        })
    }

    pub async fn render_frame(
        &self,
        job: BlenderJob,
        frame_index: usize,
    ) -> Result<()> {
        /*
         * Parse path to .blend project file
         */
        let blender_file_path = parse_with_base_directory_prefix(
            &job.project_file_path,
            Some(&self.base_directory_path),
        )?;

        // Ensure blender project file exists.
        if !blender_file_path.is_file() {
            return Err(miette!(
                "Invalid blender project file path: file doesn't exist: {:?}",
                blender_file_path
            ));
        }

        let blender_file_path_str =
            blender_file_path.to_string_lossy().to_string();

        /*
         * Parse output file
         */
        let output_file_path_str = {
            let output_directory = parse_with_base_directory_prefix(
                &job.output_directory_path,
                Some(&self.base_directory_path),
            )?;

            if !output_directory.is_dir() {
                create_dir_all(&output_directory)
                    .into_diagnostic()
                    .wrap_err_with(|| {
                        miette!("Could not create missing directories.")
                    })?;
            }

            let mut output_path = output_directory.to_string_lossy().to_string();
            output_path.push_str(&job.output_file_name_format);

            output_path
        };

        info!("Starting to render frame {}.", frame_index);

        let time_render_start = Instant::now();

        Command::new(&self.blender_binary_path)
            .args([
                &blender_file_path_str,
                "--background",
                "--render-output",
                &output_file_path_str,
                "--render-format",
                "PNG",
                "--render-frame",
                &frame_index.to_string(),
            ])
            .output()
            .await
            .into_diagnostic()
            .wrap_err_with(|| {
                miette!("Failed while executing Blender binary.")
            })?;

        let render_duration = time_render_start.elapsed();

        info!(
            "Rendered frame {} in {:.4} seconds.",
            frame_index,
            render_duration.as_secs_f64()
        );

        Ok(())
    }
}

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
            // TODO
            if !is_currently_running.load(Ordering::SeqCst) {
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
                        tokio::spawn(
                            Self::render_frame_and_report_through_websocket(
                                runner.clone(),
                                message_sender.clone(),
                                is_currently_running.clone(),
                                frames.clone(),
                                frame.job.clone(),
                                frame.frame_index,
                            ),
                        );
                    }
                }
            }

            tokio::time::sleep(Duration::from_secs_f64(0.5f64)).await;
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
                .find(|frame| {
                    frame.job == job && frame.frame_index == frame_index
                })
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
                let item_finished_message: WebSocketMessage =
                    WorkerFrameQueueItemFinishedNotification::new(
                        job_name,
                        frame_index,
                    )
                    .into();

                if let Err(error) = item_finished_message.send(&message_sender) {
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
                .position(|frame| {
                    frame.job == job && frame.frame_index == frame_index
                })
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

    pub async fn unqueue_frame(&self, job_name: String, frame_index: usize) {
        let mut locked_frames_vec = self.frames.lock().await;

        // FIXME This could potentially un-queue frames that have already started.

        locked_frames_vec.retain(|frame| {
            frame.job.job_name != job_name || frame.frame_index != frame_index
        });
    }
}
