use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures_util::future::{try_join, try_join3};
use futures_util::StreamExt;
use log::{debug, info};
use miette::Result;
use miette::{miette, Context, IntoDiagnostic};
use shared::jobs::BlenderJob;
use shared::messages::handshake::{
    MasterHandshakeAcknowledgement,
    MasterHandshakeRequest,
};
use shared::messages::queue::MasterFrameQueueAddRequest;
use shared::messages::WebSocketMessage;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, tungstenite};

use crate::jobs::MasterBlenderJobState;
use crate::websockets::{
    handle_incoming_client_messages,
    handle_outgoing_server_messages,
    Client,
    ConnectionState,
};

pub struct ClusterManager {
    server_socket: TcpListener,

    client_map: Arc<tokio::sync::Mutex<HashMap<SocketAddr, Client>>>,

    job_state: Arc<tokio::sync::Mutex<MasterBlenderJobState>>,

    job: BlenderJob,
}

impl ClusterManager {
    pub async fn new_from_job(job: BlenderJob) -> Result<Self> {
        let server_socket =
            TcpListener::bind("0.0.0.0:9901").await.into_diagnostic()?;

        Ok(Self {
            server_socket,
            client_map: Arc::new(tokio::sync::Mutex::new(
                HashMap::with_capacity(job.wait_for_number_of_workers),
            )),
            job_state: Arc::new(tokio::sync::Mutex::new(
                MasterBlenderJobState::from_job(job.clone()),
            )),
            job,
        })
    }

    pub async fn run_server_and_job_to_completion(&mut self) -> Result<()> {
        // Keep accepting connections as long as possible.
        let connection_acceptor =
            self.indefinitely_accept_and_await_connections();
        let processing_loop = self.wait_for_readiness_and_complete_job();

        try_join(connection_acceptor, processing_loop).await?;

        Ok(())
    }

    async fn indefinitely_accept_and_await_connections(&self) -> Result<()> {
        loop {
            let (stream, address) = self
                .server_socket
                .accept()
                .await
                .into_diagnostic()
                .wrap_err_with(|| {
                    miette!("Could not accept socket connection.")
                })?;

            tokio::spawn(Self::accept_client_and_perform_handshake(
                self.client_map.clone(),
                self.job_state.clone(),
                stream,
                address,
            ));
        }
    }

    async fn accept_client_and_perform_handshake<'a>(
        client_map: Arc<tokio::sync::Mutex<HashMap<SocketAddr, Client>>>,
        job_state: Arc<tokio::sync::Mutex<MasterBlenderJobState>>,
        tcp_stream: TcpStream,
        address: SocketAddr,
    ) -> Result<()> {
        debug!("[{address:?}] Accepting WebSocket connection.");
        let websocket_stream =
            accept_async(tcp_stream).await.into_diagnostic()?;
        info!("[{address:?}] Client accepted.");

        // Put the just-accepted client into the client map.
        {
            let client_map_locked = &mut client_map.lock().await;
            client_map_locked.insert(address, Client::new(address));
        }

        // Split the WebSocket stream into two parts: a sink for writing (sending) messages
        // and a stream for reading (receiving) messages.
        let (ws_write, ws_read) = websocket_stream.split();

        // To send messages through the WebSocket, send a Message instance through this unbounded channel.
        let (ws_sender_tx, ws_sender_rx) =
            futures_channel::mpsc::unbounded::<tungstenite::Message>();
        // To see received messages, read this channel.
        let (ws_receiver_tx, ws_receiver_rx) =
            futures_channel::mpsc::unbounded::<WebSocketMessage>();

        // Prepare sending and receiving futures.
        let send_queued_messages =
            handle_outgoing_server_messages(ws_write, ws_sender_rx);
        let receive_and_handle_messages =
            handle_incoming_client_messages(ws_read, ws_receiver_tx);

        let ws_sender_tx_arc = Arc::new(ws_sender_tx);
        let ws_receiver_rx_arc =
            Arc::new(tokio::sync::Mutex::new(ws_receiver_rx));

        // Spawn main handler.
        let client_handshake_handler =
            Self::perform_handshake_with_client_and_maintain_connection(
                client_map.clone(),
                job_state.clone(),
                address,
                ws_sender_tx_arc,
                ws_receiver_rx_arc,
            );

        info!("[{address:?}] Running main connection handler.");
        try_join3(
            send_queued_messages,
            receive_and_handle_messages,
            client_handshake_handler,
        )
        .await?;

        Ok(())
    }

    async fn wait_for_readiness_and_complete_job(&self) -> Result<()> {
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
                    let frame_index =
                        match job_state_locked.next_frame_to_render() {
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
                        MasterFrameQueueAddRequest::new(
                            self.job.clone(),
                            frame_index,
                        )
                        .into();

                    let client_sender = worker.sender()?;
                    queue_request.send(&client_sender)?;

                    job_state_locked
                        .mark_frame_as_queued_on_worker(frame_index)?;
                }
            }

            drop(client_map_locked);
            drop(job_state_locked);

            tokio::time::sleep(Duration::from_secs_f64(0.25f64)).await;
        }
    }

    async fn perform_handshake_with_client_and_maintain_connection(
        client_map: Arc<tokio::sync::Mutex<HashMap<SocketAddr, Client>>>,
        job_state: Arc<tokio::sync::Mutex<MasterBlenderJobState>>,
        address: SocketAddr,
        message_sender: Arc<UnboundedSender<tungstenite::Message>>,
        message_receiver: Arc<
            tokio::sync::Mutex<UnboundedReceiver<WebSocketMessage>>,
        >,
    ) -> Result<()> {
        info!("[{address:?}] Sending handshake request.");

        let handshake_request: WebSocketMessage =
            MasterHandshakeRequest::new("1.0.0").into();
        handshake_request.send(&message_sender).wrap_err_with(|| {
            miette!("Could not send initial handshake request.")
        })?;

        let handshake_response = {
            let next_message = {
                let mut locked_receiver = message_receiver.lock().await;

                locked_receiver
                    .next()
                    .await
                    .ok_or_else(|| miette!("No handshake response received."))?
            };

            match next_message {
                WebSocketMessage::WorkerHandshakeResponse(response) => response,
                _ => {
                    return Err(miette!(
                        "Invalid handshake response: not handshake_response!"
                    ));
                }
            }
        };

        info!(
            "[{address:?}] Got handshake response: worker_version={}",
            handshake_response.worker_version
        );

        let handshake_ack: WebSocketMessage =
            MasterHandshakeAcknowledgement::new(true).into();
        handshake_ack.send(&message_sender).wrap_err_with(|| {
            miette!("Could not send handshake acknowledgement.")
        })?;

        {
            let client_map_locked = &mut client_map.lock().await;

            let client = client_map_locked
                .get_mut(&address)
                .ok_or_else(|| miette!("Missing client!"))?;

            client.set_connection_state(ConnectionState::Connected {
                sender: message_sender.clone(),
                receiver: message_receiver.clone(),
            });
        }

        info!("[{address:?}] Handshake complete - fully connected!");

        loop {
            let next_message = {
                let mut locked_receiver = message_receiver.lock().await;

                locked_receiver
                    .next()
                    .await
                    .ok_or_else(|| miette!("Could not receive message."))?
            };

            match next_message {
                WebSocketMessage::WorkerFrameQueueItemFinishedNotification(
                    notification,
                ) => {
                    info!(
                        "Frame {} has finished rendering on worker.",
                        notification.frame_index
                    );

                    let client_map_locked = &mut client_map.lock().await;
                    let job_state_locked = &mut job_state.lock().await;

                    let client = client_map_locked
                        .get_mut(&address)
                        .ok_or_else(|| miette!("Missing client!"))?;
                    client.remove_from_queue(
                        notification.job_name,
                        notification.frame_index,
                    );

                    job_state_locked
                        .set_frame_completed(notification.frame_index)?;
                }
                _ => {
                    return Err(miette!("Invalid worker message."));
                }
            }
        }
    }
}
