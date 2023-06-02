use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use log::{debug, info, trace};
use miette::Result;
use miette::{miette, Context, IntoDiagnostic};
use shared::cancellation::CancellationToken;
use shared::jobs::BlenderJob;
use shared::messages::job::MasterJobFinishedEvent;
use shared::messages::traits::IntoWebSocketMessage;
use tokio::net::{TcpListener, TcpStream};

use crate::cluster::state::ClusterManagerState;
use crate::connection::Worker;

pub mod state;


pub struct ClusterManager {
    server_socket: TcpListener,

    state: Arc<ClusterManagerState>,

    job: BlenderJob,

    cancellation_token: CancellationToken,
}

impl ClusterManager {
    pub async fn new_from_job(job: BlenderJob) -> Result<Self> {
        let server_socket = TcpListener::bind("0.0.0.0:9901").await.into_diagnostic()?;
        let cancellation_token = CancellationToken::new();

        Ok(Self {
            server_socket,
            state: Arc::new(ClusterManagerState::new_from_job(job.clone())),
            job,
            cancellation_token,
        })
    }

    pub async fn run_job_to_completion(self) -> Result<()> {
        // Keep accepting connections as long as possible.
        let worker_connection_handler = Self::indefinitely_accept_connections(
            self.server_socket,
            self.state.clone(),
            self.cancellation_token.clone(),
        );

        let job_processing_loop =
            Self::wait_for_readiness_and_complete_job(self.job, self.state.clone());

        let connection_acceptor_handle = tokio::spawn(worker_connection_handler);
        job_processing_loop.await?;

        trace!("Sending job finished events to all workers.");
        {
            let locked_workers = self.state.workers.lock().await;
            for worker in locked_workers.values() {
                MasterJobFinishedEvent::new()
                    .into_ws_message()
                    .send(&worker.sender_channel)
                    .wrap_err_with(|| miette!("Could not send job finished event to worker."))?;
            }
        }

        // This should give us enough time to send out all the job finished events.
        // Otherwise we'd have to add additional complexity.
        tokio::time::sleep(Duration::from_secs(2)).await;

        trace!("Setting cancellation token to true.");
        self.cancellation_token.cancel();

        trace!("Waiting for connection acceptor to join worker connections and stop.");
        connection_acceptor_handle.await.into_diagnostic()??;

        Ok(())
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

            if cancellation_token.cancelled() {
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
        // TODO What happens when the worker disconnects?
        let worker = Worker::new_with_accept_and_handshake(
            tcp_stream,
            address,
            state.clone(),
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

    async fn wait_for_readiness_and_complete_job(
        job: BlenderJob,
        state: Arc<ClusterManagerState>,
    ) -> Result<()> {
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

            debug!(
                "{}/{} clients connected - waiting before starting job.",
                num_clients, job.wait_for_number_of_workers
            );

            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        let num_clients = {
            let workers_locked = &state.workers.lock().await;
            workers_locked.len()
        };

        info!(
            "READY! {} clients connected, starting job.",
            num_clients
        );

        loop {
            trace!("Checking if all frames have been finished.");
            if state.all_frames_finished().await {
                break;
            }

            // Queue frames onto worker that don't have any queued frames yet.
            trace!("Locking worker list and distributing pending frames.");
            let mut workers_locked = state.workers.lock().await;
            for worker in workers_locked.values_mut() {
                if worker.has_empty_queue().await {
                    trace!(
                        "Worker {} has empty queue, trying to queue.",
                        worker.address
                    );

                    /*
                     * Find next pending frame and queue it on this worker (if available).
                     */

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

            tokio::time::sleep(Duration::from_secs_f64(0.25f64)).await;
        }

        info!("All frames have been finished!");
        Ok(())
    }
}
