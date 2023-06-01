use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::try_join;
use log::{debug, info, trace};
use miette::Result;
use miette::{miette, Context, IntoDiagnostic};
use shared::jobs::BlenderJob;
use tokio::net::{TcpListener, TcpStream};

use crate::cluster::state::ClusterManagerState;
use crate::worker::Worker;

pub mod state;


pub struct ClusterManager {
    server_socket: TcpListener,

    state: Arc<ClusterManagerState>,

    job: BlenderJob,
}

impl ClusterManager {
    pub async fn new_from_job(job: BlenderJob) -> Result<Self> {
        let server_socket = TcpListener::bind("0.0.0.0:9901").await.into_diagnostic()?;

        Ok(Self {
            server_socket,
            state: Arc::new(ClusterManagerState::new_from_job(job.clone())),
            job,
        })
    }

    pub async fn run_job_to_completion(&mut self) -> Result<()> {
        // Keep accepting connections as long as possible.
        let connection_acceptor = self.indefinitely_accept_connections();

        let processing_loop = self.wait_for_readiness_and_complete_job();

        try_join(connection_acceptor, processing_loop)
            .await
            .wrap_err_with(|| {
                miette!("Errored while running connection acceptor / processing loop.")
            })?;

        Ok(())
    }

    async fn indefinitely_accept_connections(&self) -> Result<()> {
        loop {
            let (stream, address) = self
                .server_socket
                .accept()
                .await
                .into_diagnostic()
                .wrap_err_with(|| miette!("Could not accept socket connection."))?;

            tokio::spawn(Self::accept_worker(
                self.state.clone(),
                address,
                stream,
            ));
        }
    }

    async fn accept_worker(
        state: Arc<ClusterManagerState>,
        address: SocketAddr,
        tcp_stream: TcpStream,
    ) -> Result<()> {
        let worker = Worker::new_with_accept_and_handshake(tcp_stream, address, state.clone())
            .await
            .wrap_err_with(|| miette!("Could not create worker."))?;

        // Put the just-accepted client into the client map.
        {
            let workers_locked = &mut state.workers.lock().await;
            workers_locked.insert(address, worker);
        }

        // TODO What happens when the worker disconnects?

        Ok(())
    }

    async fn wait_for_readiness_and_complete_job(&self) -> Result<()> {
        info!(
            "Waiting for at least {} workers to connect before starting job.",
            self.job.wait_for_number_of_workers
        );

        loop {
            let num_clients = {
                let workers_locked = &self.state.workers.lock().await;
                workers_locked.len()
            };

            if num_clients >= self.job.wait_for_number_of_workers {
                break;
            }

            debug!(
                "{}/{} clients connected - waiting before starting job.",
                num_clients, self.job.wait_for_number_of_workers
            );

            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        let num_clients = {
            let workers_locked = &self.state.workers.lock().await;
            workers_locked.len()
        };

        info!(
            "READY! {} clients connected, starting job.",
            num_clients
        );

        loop {
            trace!("Checking if all frames have been finished.");
            if self.state.all_frames_finished().await {
                info!("All frames have been finished!");
                return Ok(());
            }

            // Queue frames onto worker that don't have any queued frames yet.
            trace!("Locking worker list and distributing pending frames.");
            let mut workers_locked = self.state.workers.lock().await;
            for worker in workers_locked.values_mut() {
                if worker.has_empty_queue().await {
                    trace!(
                        "Worker {} has empty queue, trying to queue.",
                        worker.address
                    );

                    // Find next pending frame and queue it on this worker (if available).
                    let next_frame_index = match self.state.next_pending_frame().await {
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
                        .queue_frame(self.job.clone(), next_frame_index)
                        .await?;
                    self.state
                        .mark_frame_as_queued_on_worker(worker.address, next_frame_index)
                        .await?;
                }
            }
            drop(workers_locked);

            tokio::time::sleep(Duration::from_secs_f64(0.25f64)).await;
        }
    }
}
