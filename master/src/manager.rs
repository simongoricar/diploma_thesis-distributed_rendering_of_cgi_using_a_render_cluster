use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures_util::future::{try_join, try_join3, TryJoin3};
use futures_util::{future, pin_mut, StreamExt};
use log::{debug, info};
use miette::Result;
use miette::{miette, Context, IntoDiagnostic};
use shared::jobs::{BlenderJob, BlenderJobState};
use shared::messages::handshake::{
    MasterHandshakeAcknowledgement,
    MasterHandshakeRequest,
};
use shared::messages::WebSocketMessage;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, tungstenite};

use crate::websockets::{
    handle_incoming_client_messages,
    handle_outgoing_server_messages,
    Client,
    ConnectionState,
};

pub struct ManagerState {
    client_map: HashMap<SocketAddr, Client>,

    job_state: BlenderJobState,
}

impl ManagerState {
    pub fn new(job: &BlenderJob) -> Self {
        let client_map = HashMap::new();
        let job_state = BlenderJobState::from_job(job);

        Self {
            client_map,
            job_state,
        }
    }
}

pub type SharedManagerState = Arc<std::sync::Mutex<ManagerState>>;

pub struct ClusterManager {
    server_socket: TcpListener,

    state: SharedManagerState,

    job: BlenderJob,
}

impl ClusterManager {
    pub async fn new_from_job(job: BlenderJob) -> Result<Self> {
        let server_socket =
            TcpListener::bind("0.0.0.0:9901").await.into_diagnostic()?;

        let shared_state = SharedManagerState::new(std::sync::Mutex::new(
            ManagerState::new(&job),
        ));

        Ok(Self {
            server_socket,
            state: shared_state,
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

            let new_client_future = Self::accept_client_and_perform_handshake(
                self.state.clone(),
                stream,
                address,
            )
            .await?;
        }
    }

    async fn accept_client_and_perform_handshake<'a>(
        state: SharedManagerState,
        tcp_stream: TcpStream,
        address: SocketAddr,
    ) -> Result<()> {
        debug!("[{address:?}] Accepting WebSocket connection.");
        let websocket_stream =
            accept_async(tcp_stream).await.into_diagnostic()?;
        info!("[{address:?}] Client accepted.");

        // Put the just-accepted client into the client map.
        {
            let client_map = &mut state
                .lock()
                .expect("Shared state Mutex has been poisoned.")
                .client_map;

            client_map.insert(address, Client::new(address));
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
        let client_handshake_handler = Self::perform_handshake_with_client(
            state.clone(),
            address,
            ws_sender_tx_arc,
            ws_receiver_rx_arc,
        );

        let super_future = try_join3(
            send_queued_messages,
            receive_and_handle_messages,
            client_handshake_handler,
        );

        info!("[{address:?}] Running main connection handler.");
        tokio::spawn(super_future);

        Ok(())

        // Remove the lost client from the client map.
        // {
        //     let client_map = &mut state
        //         .lock()
        //         .expect("Client map lock has been poisoned.")
        //         .client_map;
        //
        //     client_map.remove(&address);
        // }
        //
        // info!("Client {:?} disconnected.", address);
        // Ok(())
    }

    async fn wait_for_readiness_and_complete_job(&self) -> Result<()> {
        info!(
            "Waiting for at least {} workers before starting job.",
            self.job.wait_for_number_of_workers
        );

        loop {
            let num_clients = {
                let clients_map = &self
                    .state
                    .lock()
                    .expect("Shared state Mutex has been poisoned!")
                    .client_map;

                clients_map.len()
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

        // TODO
        Ok(())
    }

    async fn perform_handshake_with_client(
        state: SharedManagerState,
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
            let map = &mut state
                .lock()
                .expect("Shared state Mutex has been poisoned.")
                .client_map;

            let client = map
                .get_mut(&address)
                .ok_or_else(|| miette!("Missing client!"))?;

            client.set_connection_state(ConnectionState::Connected {
                sender: message_sender.clone(),
                receiver: message_receiver.clone(),
            });
        }

        info!("[{address:?}] Handshake complete - fully connected!");

        // TODO

        Ok(())
    }
}
