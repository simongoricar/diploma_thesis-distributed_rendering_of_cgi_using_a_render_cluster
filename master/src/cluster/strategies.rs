use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use log::{info, trace};
use miette::{miette, Result};
use shared::jobs::{BlenderJob, DynamicStrategyOptions};
use shared::messages::queue::FrameQueueRemoveResult;

use crate::cluster::state::ClusterManagerState;
use crate::connection::queue::FrameOnWorker;
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

                worker
                    .queue_frame(job.clone(), next_frame_index, None)
                    .await?;

                shared_state
                    .mark_frame_as_queued_on_worker(worker.address, next_frame_index, None)
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

                    worker
                        .queue_frame(job.clone(), next_frame_index, None)
                        .await?;

                    trace!(
                        "Frame {} has been queued on worker.",
                        next_frame_index
                    );

                    shared_state
                        .mark_frame_as_queued_on_worker(worker.address, next_frame_index, None)
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

/*
 * Dynamic distribution strategy
 */
fn select_best_frame_to_steal<'a>(
    worker_address: SocketAddr,
    worker_frame_queue: &'a [FrameOnWorker],
    options: &DynamicStrategyOptions,
) -> Option<&'a FrameOnWorker> {
    // Find best frame to steal, i.e. a yet un-stolen frame or the frame that
    // has been on this worker for the longest time.
    // We meanwhile respect these options:
    //   - we don't steal the first `options.min_queue_size_to_steal` frames (ones nearest to rendering),
    //   - we don't steal frames that had been reassigned to this worker in the last `options.min_seconds_before_resteal_to_elsewhere` seconds,
    //   - we don't steal frames back to the worker it was stolen from (unless `options.min_seconds_before_resteal_to_original_worker` seconds has passed).

    let mut least_stolen_frame: Option<&FrameOnWorker> = None;
    for frame in worker_frame_queue
        .iter()
        .skip(options.min_queue_size_to_steal)
        .rev()
    {
        let since_queued = frame.queued_at.elapsed();
        if let Some(previous_worker_address) = frame.stolen_from.as_ref() {
            if previous_worker_address == &worker_address
                && since_queued.as_secs_f64()
                    >= options.min_seconds_before_resteal_to_original_worker as f64
            {
                least_stolen_frame = Some(frame);
                continue;
            }
        }

        if since_queued.as_secs_f64() >= options.min_seconds_before_resteal_to_elsewhere as f64 {
            least_stolen_frame = Some(frame);
        }
    }

    least_stolen_frame
}

async fn find_busiest_worker_and_frame_to_steal_from<'a>(
    worker_address: SocketAddr,
    workers_list: Vec<&'a Worker>,
    options: &DynamicStrategyOptions,
) -> Option<(&'a Worker, usize, FrameOnWorker)> {
    // Compute the busiest worker (the one with the longest queue).
    // We also respect the `min_queue_size_to_steal` parameter (we don't want to steal
    // from any short queue when *all* the queues are very short).
    let mut busiest_worker: Option<(&'a Worker, usize, FrameOnWorker)> = None;
    for other_worker in workers_list {
        // We shouldn't steal frame from and give back to the same worker.
        if other_worker.address == worker_address {
            continue;
        }

        let other_worker_queue_locked = other_worker.queue.lock().await;

        let other_worker_queue_size = other_worker_queue_locked.num_queued_frames();

        if let Some((_, best_worker_queue_size, _)) = &busiest_worker {
            // Update the busiest worker if the current worker has a bigger queue and a potential frame to steal.
            if other_worker_queue_size > *best_worker_queue_size {
                let best_frame_to_steal = select_best_frame_to_steal(
                    worker_address,
                    &other_worker_queue_locked.queue,
                    options,
                );

                if let Some(best_frame) = best_frame_to_steal {
                    busiest_worker = Some((
                        other_worker,
                        other_worker_queue_size,
                        best_frame.clone(),
                    ));
                }
            }
        } else if other_worker_queue_size > options.min_queue_size_to_steal {
            // If no busiest worker yet, set one, but only if it has a large enough queue to warrant stealing from.
            let best_frame_to_steal = select_best_frame_to_steal(
                worker_address,
                &other_worker_queue_locked.queue,
                options,
            );

            if let Some(best_frame) = best_frame_to_steal {
                busiest_worker = Some((
                    other_worker,
                    other_worker_queue_size,
                    best_frame.clone(),
                ));
            }
        }
    }

    busiest_worker
}

pub async fn dynamic_distribution_strategy(
    job: &BlenderJob,
    shared_state: Arc<ClusterManagerState>,
    options: DynamicStrategyOptions,
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

        // Create a vector of worker references sorted in ascending order (by their queue size).
        let mut workers_by_ascending_queue_size: Vec<&Worker> = workers_locked.values().collect();
        workers_by_ascending_queue_size.sort_unstable_by(|first, second| {
            let first_queue_size = first.queue_size.load(Ordering::SeqCst);
            let second_queue_size = second.queue_size.load(Ordering::SeqCst);

            first_queue_size.cmp(&second_queue_size)
        });

        // Iterate over all workers, starting with those with the shortest queue.
        // We'll try to assign an unqueued frame to them or, if that is not possible,
        // attempt to "steal" a queued frame from some full worker.
        for worker in workers_by_ascending_queue_size {
            let worker_num_queued_frames = {
                let worker_queue_locked = worker.queue.lock().await;
                worker_queue_locked.num_queued_frames()
            };

            // Try to queue a new frame onto a worker if it is below the target queue size.
            if worker_num_queued_frames < options.target_queue_size {
                // Queues just one new frame at a time - we'll reach equilibrium eventually,
                // but this way will be faster across multiple workers.
                if let Some(next_unqueued_frame) = shared_state.next_pending_frame().await {
                    // Queue globally unqueued frame.
                    info!(
                        "Worker {}: queueing globally unqueued frame ({}).",
                        worker.address, next_unqueued_frame,
                    );

                    worker
                        .queue_frame(job.clone(), next_unqueued_frame, None)
                        .await?;

                    shared_state
                        .mark_frame_as_queued_on_worker(worker.address, next_unqueued_frame, None)
                        .await?;
                } else {
                    // All frames have been queued already, steal from some other worker instead.
                    // We prefer frames that haven't been stolen yet or have been stolen long ago,
                    // and also prefer workers with longest queues.
                    // See `find_busiest_worker_and_frame_to_steal_from` for more information.
                    let busiest_worker = find_busiest_worker_and_frame_to_steal_from(
                        worker.address,
                        workers_locked.values().collect::<Vec<&Worker>>(),
                        &options,
                    )
                    .await;

                    let (busiest_worker, best_frame) = match busiest_worker {
                        Some((worker, _, best_frame)) => (worker, best_frame),
                        None => {
                            // In this case there is no suitable worker to steal from, so we simply
                            // don't steal and move on.
                            break;
                        }
                    };

                    // Unqueue the frame from the previous worker
                    // and queue it on the one with the shorter queue.
                    info!(
                        "Worker {}: global unqueued pool is empty, stealing frame {} from worker {} instead.",
                        worker.address,
                        best_frame.frame_index,
                        busiest_worker.address,
                    );

                    let unqueue_result = busiest_worker
                        .unqueue_frame(
                            best_frame.job.job_name.clone(),
                            best_frame.frame_index,
                        )
                        .await?;

                    match unqueue_result {
                        FrameQueueRemoveResult::RemovedFromQueue => {}
                        FrameQueueRemoveResult::AlreadyRendering => {
                            // An issue of latency, simply ignore the "error".
                            info!(
                                "Worker {}: can't steal, frame {} has already started rendering on original worker.",
                                worker.address,
                                best_frame.frame_index,
                            );
                            continue;
                        }
                        FrameQueueRemoveResult::AlreadyFinished => {
                            // An issue of latency, simply ignore the "error".
                            info!(
                                "Worker {}: can't steal, frame {} has already finished on original worker.",
                                worker.address,
                                best_frame.frame_index,
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

                    // Queue onto the more "starved" worker.
                    worker
                        .queue_frame(
                            best_frame.job,
                            best_frame.frame_index,
                            Some(busiest_worker.address),
                        )
                        .await?;

                    shared_state
                        .mark_frame_as_queued_on_worker(
                            worker.address,
                            best_frame.frame_index,
                            Some(busiest_worker.address),
                        )
                        .await?;


                    info!(
                        "Worker {}: frame {} successfully reassigned from {} to itself.",
                        worker.address, best_frame.frame_index, busiest_worker.address,
                    );
                }
            }
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    Ok(())
}
