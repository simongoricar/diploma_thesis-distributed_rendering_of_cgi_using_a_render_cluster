use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use log::{info, trace};
use miette::{miette, Result};
use shared::jobs::BlenderJob;
use shared::messages::queue::FrameQueueRemoveResult;

use crate::cluster::state::ClusterManagerState;
use crate::connection::Worker;


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
    target_queue_size: usize,
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

            if worker_num_queued_frames < target_queue_size {
                let frames_to_limit = target_queue_size - worker_num_queued_frames;
                trace!(
                    "Worker {} has {} (< {}) queued frames, trying to top up.",
                    worker.address,
                    worker_num_queued_frames,
                    target_queue_size
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

pub async fn dynamic_distribution_strategy(
    job: &BlenderJob,
    shared_state: Arc<ClusterManagerState>,
    target_queue_size: usize,
    min_queue_size_to_steal: usize,
) -> Result<()> {
    loop {
        trace!("Checking if all frames have been finished.");
        if shared_state.all_frames_finished().await {
            break;
        }

        let workers_locked = shared_state.workers.lock().await;

        // Pseudocode:
        //   distribute up to `target_queue_size` frames to each worker, one by one until reaching equilibrium
        //
        //   then always operate on workers with shortest queues first:
        //
        //     if a workers' queue size is below `target_queue_size`, attempt to queue additional frames onto it
        //     from the global pending pool
        //       if no frames are pending (i.e. all queued/rendering), then our only option is to "steal" queued frames
        //       from other workers:
        //         take frames from workers with longest queue and reassign them to the ones with the shortest

        let mut workers_by_ascending_queue_size: Vec<&Worker> = workers_locked.values().collect();
        workers_by_ascending_queue_size.sort_unstable_by(|first, second| {
            let first_queue_size = first.queue_size.load(Ordering::SeqCst);
            let second_queue_size = second.queue_size.load(Ordering::SeqCst);

            first_queue_size.cmp(&second_queue_size)
        });

        for worker in workers_by_ascending_queue_size {
            let worker_num_queued_frames = {
                let worker_queue_locked = worker.queue.lock().await;
                worker_queue_locked.num_queued_frames()
            };

            if worker_num_queued_frames < target_queue_size {
                // Queue just one at a time - we'll reach equilibrium eventually,
                // but this way will be faster.

                if let Some(next_unqueued_frame) = shared_state.next_pending_frame().await {
                    // Queue globally unqueued frame.
                    info!(
                        "Worker {}: queueing globally unqueued frame ({}).",
                        worker.address, next_unqueued_frame,
                    );

                    worker.queue_frame(job.clone(), next_unqueued_frame).await?;

                    shared_state
                        .mark_frame_as_queued_on_worker(worker.address, next_unqueued_frame)
                        .await?;
                } else {
                    // Steal from some other worker instead.

                    // Compute the busiest worker (the one with the longest queue).
                    // We also respect the `min_queue_size_to_steal` parameter (we don't want to steal
                    // from any short queue when *all* the queues are very short).
                    let mut busiest_worker: Option<(&Worker, usize)> = None;
                    for other_worker in workers_locked.values() {
                        // We shouldn't steal frame from and give back to the same worker.
                        if other_worker.address == worker.address {
                            continue;
                        }

                        let other_worker_queue_size = {
                            let queue_locked = other_worker.queue.lock().await;
                            queue_locked.num_queued_frames()
                        };

                        if let Some((_, best_worker_queue_size)) = &busiest_worker {
                            if other_worker_queue_size > *best_worker_queue_size {
                                busiest_worker = Some((other_worker, other_worker_queue_size))
                            }
                        } else if other_worker_queue_size > min_queue_size_to_steal {
                            busiest_worker = Some((other_worker, other_worker_queue_size));
                        }
                    }

                    let busiest_worker = match busiest_worker {
                        Some((worker, _)) => worker,
                        None => {
                            break;
                        }
                    };

                    // Unqueue the frame from the previous worker
                    // and queue it on the one with the shorter queue.
                    let last_queued_frame = {
                        let locked_queue = busiest_worker.queue.lock().await;
                        locked_queue.last().ok_or_else(|| {
                            miette!("BUG: Invalid empty queue - how is the busiest worker empty!?")
                        })?
                    };

                    info!(
                        "Worker {}: global unqueued pool is empty, stealing frame {} from worker {} instead.",
                        worker.address,
                        last_queued_frame.frame_index,
                        busiest_worker.address,
                    );

                    let unqueue_result = busiest_worker
                        .unqueue_frame(
                            last_queued_frame.job.job_name.clone(),
                            last_queued_frame.frame_index,
                        )
                        .await?;

                    match unqueue_result {
                        FrameQueueRemoveResult::RemovedFromQueue => {}
                        FrameQueueRemoveResult::AlreadyRendering => {
                            // An issue of latency, simply ignore the "error".
                            info!(
                                "Worker {}: can't steal, frame {} has already started rendering on original worker.",
                                worker.address,
                                last_queued_frame.frame_index,
                            );
                            continue;
                        }
                        FrameQueueRemoveResult::AlreadyFinished => {
                            // An issue of latency, simply ignore the "error".
                            info!(
                                "Worker {}: can't steal, frame {} has already finished on original worker.",
                                worker.address,
                                last_queued_frame.frame_index,
                            );
                            continue;
                        }
                        FrameQueueRemoveResult::Errored { reason } => {
                            return Err(miette!(
                                "Worker errored while trying to remove item from queue: {}",
                                reason
                            ));
                        }
                    }

                    // Queue onto first worker.
                    worker
                        .queue_frame(
                            last_queued_frame.job,
                            last_queued_frame.frame_index,
                        )
                        .await?;

                    shared_state
                        .mark_frame_as_queued_on_worker(
                            worker.address,
                            last_queued_frame.frame_index,
                        )
                        .await?;


                    info!(
                        "Worker {}: frame {} successfully reassigned to self.",
                        worker.address, last_queued_frame.frame_index,
                    );
                }
            }
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    Ok(())
}
