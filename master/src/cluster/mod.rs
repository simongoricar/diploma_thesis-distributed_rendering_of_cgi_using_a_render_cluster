use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use log::{info, trace};
use miette::Result;
use miette::{miette, Context, IntoDiagnostic};
use shared::cancellation::CancellationToken;
use shared::jobs::BlenderJob;
use shared::messages::job::MasterJobStartedEvent;
use shared::results::performance::MasterPerformance;
use shared::results::worker_trace::WorkerTrace;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::Instant;

use crate::cluster::state::ClusterManagerState;
use crate::connection::Worker;

pub mod state;


pub struct ClusterManager {}

impl ClusterManager {
    pub async fn initialize_server_and_run_job(
        host: &str,
        port: usize,
        job: BlenderJob,
    ) -> Result<(MasterPerformance, Vec<(SocketAddr, WorkerTrace)>)> {
        let shared_state = Arc::new(ClusterManagerState::new_from_job(job.clone()));
        let cancellation_token = CancellationToken::new();

        let server_socket = TcpListener::bind(format!("{host}:{port}"))
            .await
            .into_diagnostic()?;

        // Initialize two futures: one that accepts incoming worker connections
        // and another that waits for correct amount of workers and performs the job.
        let worker_connection_acceptor_future = Self::indefinitely_accept_connections(
            server_socket,
            shared_state.clone(),
            cancellation_token.clone(),
        );

        let job_processing_future = Self::wait_for_workers_and_run_job(job, shared_state.clone());

        // Spawn the acceptor in the background and wait for the job runner to complete first.
        let worker_connection_acceptor_handle = tokio::spawn(worker_connection_acceptor_future);
        let master_performance = job_processing_future.await?;

        // Request performance traces from workers. Shortly after the workers respond with those,
        // they will shut themselves down.
        info!("Job finished, requesting performance traces from all workers...");
        let mut worker_traces: Vec<(SocketAddr, WorkerTrace)> = Vec::new();
        {
            let locked_workers = shared_state.workers.lock().await;

            for worker in locked_workers.values() {
                // Prevents the per-worker heartbeat task from sending any more messages through the WebSocket.
                worker.heartbeat_cancellation_token.cancel();

                let worker_trace = worker
                    .requester
                    .finish_job_and_get_trace()
                    .await
                    .wrap_err_with(|| {
                        miette!(
                            "Could not receive trace from worker {:?}!",
                            worker.address
                        )
                    })?;

                worker_traces.push((worker.address, worker_trace));
            }
        }

        trace!("Setting cancellation token...");
        cancellation_token.cancel();

        trace!("Waiting for worker connection acceptor...");
        worker_connection_acceptor_handle
            .await
            .into_diagnostic()??;

        Ok((master_performance, worker_traces))
    }

    async fn indefinitely_accept_connections(
        server_socket: TcpListener,
        state: Arc<ClusterManagerState>,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        loop {
            if let Ok(incoming_connection) =
                tokio::time::timeout(Duration::from_secs(2), server_socket.accept()).await
            {
                let (stream, address) = incoming_connection
                    .into_diagnostic()
                    .wrap_err_with(|| miette!("Could not accept incoming connection."))?;

                tokio::spawn(Self::accept_worker(
                    state.clone(),
                    address,
                    stream,
                    cancellation_token.clone(),
                ));
            }

            if cancellation_token.is_cancelled() {
                info!("Waiting for all worker connections to drop (cluster stopping).");
                {
                    let mut locked_workers = state.workers.lock().await;
                    for (address, worker) in locked_workers.drain() {
                        trace!(
                            "Joining worker: \"{}:{}\"",
                            address.ip(),
                            address.port()
                        );
                        worker.join().await?;
                    }
                }

                info!("Stopping worker connection acceptor (cluster stopping).");
                break;
            }
        }

        Ok(())
    }

    async fn accept_worker(
        state: Arc<ClusterManagerState>,
        address: SocketAddr,
        tcp_stream: TcpStream,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        let heartbeat_cancellation_token = CancellationToken::new();

        // TODO What happens when the worker disconnects?
        let worker = Worker::new_with_accept_and_handshake(
            tcp_stream,
            address,
            state.clone(),
            heartbeat_cancellation_token,
            cancellation_token,
        )
        .await
        .wrap_err_with(|| miette!("Could not create worker."))?;

        // Put the just-accepted client into the client map.
        {
            let workers_locked = &mut state.workers.lock().await;
            workers_locked.insert(address, worker);
        }

        Ok(())
    }

    async fn wait_for_workers_and_run_job(
        job: BlenderJob,
        state: Arc<ClusterManagerState>,
    ) -> Result<MasterPerformance> {
        /*
         * Wait for `job.wait_for_number_of_workers` workers to connect.
         */
        info!(
            "Waiting for at least {} workers to connect before starting job.",
            job.wait_for_number_of_workers
        );

        loop {
            let num_clients = {
                let workers_locked = &state.workers.lock().await;
                workers_locked.len()
            };

            if num_clients >= job.wait_for_number_of_workers {
                break;
            }

            trace!(
                "{}/{} workers currently connected.",
                num_clients,
                job.wait_for_number_of_workers
            );

            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        /*
         * Send job starting events to all connected workers.
         */
        let time_job_start = Instant::now();

        {
            let workers_locked = &state.workers.lock().await;

            let workers_amount = workers_locked.len();
            info!(
                "READY! {} workers are connected, starting job!",
                workers_amount
            );

            for (_, worker) in workers_locked.iter() {
                let worker_sender_handle = worker.sender.sender_handle();

                worker_sender_handle
                    .send_message(MasterJobStartedEvent::new())
                    .await
                    .wrap_err_with(|| {
                        miette!(
                            "Could not send job started event to worker {:?}",
                            worker.address
                        )
                    })?;
            }
        }

        // FIXME If a client connects after a job has started, they will not receive the job start event.

        /*
         * Run the Blender job to completion.
         */
        loop {
            trace!("Checking if all frames have been finished.");
            if state.all_frames_finished().await {
                break;
            }

            // Queue frames onto workers that don't have any queued frames yet.

            trace!("Locking worker list and distributing pending frames.");
            let mut workers_locked = state.workers.lock().await;

            for worker in workers_locked.values_mut() {
                if worker.has_empty_queue().await {
                    trace!(
                        "Worker {} has empty queue, trying to queue.",
                        worker.address
                    );

                    // Find next pending frame and queue it on this worker (if available).

                    let next_frame_index = match state.next_pending_frame().await {
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

                    state
                        .mark_frame_as_queued_on_worker(worker.address, next_frame_index)
                        .await?;
                }
            }

            drop(workers_locked);

            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        info!("All frames have been finished!");

        let total_job_duration = time_job_start.elapsed();
        let performance = MasterPerformance::new(total_job_duration);

        Ok(performance)
    }
}
