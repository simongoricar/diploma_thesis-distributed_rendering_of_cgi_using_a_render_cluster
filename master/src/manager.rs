use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures_channel::mpsc::UnboundedSender;
use futures_util::future::try_join;
use log::{debug, info};
use miette::Result;
use miette::{miette, Context, IntoDiagnostic};
use shared::jobs::BlenderJob;
use shared::messages::queue::MasterFrameQueueAddRequest;
use shared::messages::WebSocketMessage;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite;

use crate::jobs::MasterBlenderJobState;
use crate::websockets::worker::Worker;


pub struct ClusterManager {
    server_socket: TcpListener,

    client_map: Arc<Mutex<HashMap<SocketAddr, Worker>>>,

    job_state: Arc<Mutex<MasterBlenderJobState>>,

    job: BlenderJob,
}

impl ClusterManager {
    pub async fn new_from_job(job: BlenderJob) -> Result<Self> {
        let server_socket = TcpListener::bind("0.0.0.0:9901").await.into_diagnostic()?;

        let empty_client_map = Arc::new(Mutex::new(HashMap::with_capacity(
            job.wait_for_number_of_workers,
        )));
        let initial_job_state = Arc::new(Mutex::new(MasterBlenderJobState::from_job(
            job.clone(),
        )));

        Ok(Self {
            server_socket,
            client_map: empty_client_map,
            job_state: initial_job_state,
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
                self.client_map.clone(),
                address,
                stream,
            ));
        }
    }

    async fn accept_worker(
        client_map: Arc<Mutex<HashMap<SocketAddr, Worker>>>,
        address: SocketAddr,
        tcp_stream: TcpStream,
    ) -> Result<()> {
        let worker = Worker::new_with_accept_and_handshake(tcp_stream, address)
            .await
            .wrap_err_with(|| miette!("Could not create worker."))?;

        // Put the just-accepted client into the client map.
        {
            let client_map_locked = &mut client_map.lock().await;
            client_map_locked.insert(address, worker);
        }

        // TODO What happens when the worker disconnects?

        Ok(())
    }

    async fn wait_for_readiness_and_complete_job(&self) -> Result<()> {
        // TODO Rewrite this with new Worker abstraction.

        info!(
            "Waiting for at least {} workers before starting job.",
            self.job.wait_for_number_of_workers
        );

        loop {
            let num_clients = {
                let client_map_locked = &self.client_map.lock().await;
                client_map_locked.len()
            };

            if num_clients >= self.job.wait_for_number_of_workers {
                break;
            }

            debug!(
                "{}/{} clients connected - waiting before starting job.",
                num_clients, self.job.wait_for_number_of_workers
            );

            tokio::time::sleep(Duration::from_secs_f64(0.5)).await;
        }

        info!(
            "READY! At least {} clients connected, starting job.",
            self.job.wait_for_number_of_workers
        );
        
        loop {
            let mut client_map_locked = self.client_map.lock().await;
            let mut job_state_locked = self.job_state.lock().await;

            // If no frames are left to render, exit this future.
            if job_state_locked.next_frame_to_render().is_none()
                && job_state_locked.have_all_frames_finished()
            {
                info!("All frames have been rendered!");
                return Ok(());
            }
            
            // Queue frames onto worker that don't have any queued frames yet.
            for worker in client_map_locked.values_mut() {
                
            }
            
        }
        
        // DEPRECATED below, rewriting above
        loop {
            let mut client_map_locked = self.client_map.lock().await;
            let mut job_state_locked = self.job_state.lock().await;

            // If no frames are left to render, exit.
            if job_state_locked.next_frame_to_render().is_none()
                && job_state_locked.have_all_frames_finished()
            {
                info!("All frames have been rendered!");
                return Ok(());
            }

            // This is the actual queueing logic.

            // Queue frames onto workers that don't have any frames to render.

            for worker in client_map_locked.values_mut() {
                if worker.has_empty_queue() {
                    // Find next un-queued frame (break out of for loop if none left).
                    let frame_index = match job_state_locked.next_frame_to_render() {
                        Some(frame_index) => frame_index,
                        None => {
                            break;
                        }
                    };

                    // Queue a yet-un-queued frame and reflect that change in `locked_state.job_state`.
                    info!(
                        "Queueing new frame on worker: {} on {}",
                        frame_index, worker.address
                    );

                    let queue_request: WebSocketMessage =
                        MasterFrameQueueAddRequest::new(self.job.clone(), frame_index).into();

                    let client_sender = worker
                        .sender()
                        .wrap_err_with(|| miette!("Could not get worker's sender channel."))?;
                    queue_request
                        .send(&client_sender)
                        .wrap_err_with(|| miette!("Could not send queue request to worker."))?;

                    job_state_locked
                        .mark_frame_as_queued_on_worker(frame_index)
                        .wrap_err_with(|| miette!("Could not locally mark frame as queued."))?;
                }
            }

            drop(client_map_locked);
            drop(job_state_locked);

            tokio::time::sleep(Duration::from_secs_f64(0.25f64)).await;
        }
    }

    async fn maintain_heartbeats_with_worker(
        message_sender: Arc<UnboundedSender<tungstenite::Message>>,
    ) {
        // TODO
    }
}
