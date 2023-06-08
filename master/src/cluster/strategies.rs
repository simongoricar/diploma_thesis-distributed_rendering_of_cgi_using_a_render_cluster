use std::sync::Arc;
use std::time::Duration;

use log::{info, trace};
use miette::Result;
use shared::jobs::BlenderJob;

use crate::cluster::state::ClusterManagerState;


pub async fn naive_fine_distribution_strategy(
    job: &BlenderJob,
    shared_state: Arc<ClusterManagerState>,
) -> Result<()> {
    loop {
        trace!("Checking if all frames have been finished.");
        if shared_state.all_frames_finished().await {
            break;
        }

        // Queue frames onto workers that don't have any queued frames yet.

        trace!("Locking worker list and distributing pending frames.");
        let mut workers_locked = shared_state.workers.lock().await;

        for worker in workers_locked.values_mut() {
            if worker.has_empty_queue().await {
                trace!(
                    "Worker {} has empty queue, trying to queue.",
                    worker.address
                );

                // Find next pending frame and queue it on this worker (if available).

                let next_frame_index = match shared_state.next_pending_frame().await {
                    Some(frame_index) => frame_index,
                    None => {
                        break;
                    }
                };

                info!(
                    "Queueing frame {} on worker {}.",
                    next_frame_index, worker.address
                );

                worker.queue_frame(job.clone(), next_frame_index).await?;

                shared_state
                    .mark_frame_as_queued_on_worker(worker.address, next_frame_index)
                    .await?;
            }
        }

        drop(workers_locked);

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    Ok(())
}

pub async fn naive_coarse_distribution_strategy(
    job: &BlenderJob,
    shared_state: Arc<ClusterManagerState>,
    chunk_size: usize,
) -> Result<()> {
    loop {
        trace!("Checking if all frames have been finished.");
        if shared_state.all_frames_finished().await {
            break;
        }

        let mut workers_locked = shared_state.workers.lock().await;

        for worker in workers_locked.values_mut() {
            let worker_num_queued_frames = {
                let worker_queue_locked = worker.queue.lock().await;
                worker_queue_locked.num_queued_frames()
            };

            trace!(
                "Worker {} has {} queued frames.",
                worker.address,
                worker_num_queued_frames
            );

            if worker_num_queued_frames < chunk_size {
                let frames_to_limit = chunk_size - worker_num_queued_frames;
                info!(
                    "Worker {} has {} (< {}) queued frames, trying to top up.",
                    worker.address, worker_num_queued_frames, chunk_size
                );

                for _ in 0..frames_to_limit {
                    let next_frame_index = match shared_state.next_pending_frame().await {
                        Some(frame) => frame,
                        None => {
                            // No further frames to queue at all, exit this for loop.
                            break;
                        }
                    };

                    info!(
                        "Queueing frame {} on worker {}.",
                        next_frame_index, worker.address
                    );

                    worker.queue_frame(job.clone(), next_frame_index).await?;

                    trace!(
                        "Frame {} has been queued on worker.",
                        next_frame_index
                    );

                    shared_state
                        .mark_frame_as_queued_on_worker(worker.address, next_frame_index)
                        .await?;

                    trace!(
                        "Frame {} has been set as queued internally.",
                        next_frame_index
                    );
                }

                if shared_state.next_pending_frame().await.is_none() {
                    trace!("No further frames to queue, all are either rendering or finished.");
                    break;
                }
            }
        }

        drop(workers_locked);

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Ok(())
}
