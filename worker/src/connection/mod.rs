pub mod event_dispatcher;

use std::io;
use std::io::ErrorKind;
use std::sync::Arc;

use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt, TryStreamExt};
use log::{debug, info};
use miette::{miette, Context, IntoDiagnostic, Result};
use shared::messages::handshake::{
    MasterHandshakeAcknowledgement,
    MasterHandshakeRequest,
    WorkerHandshakeResponse,
};
use shared::messages::heartbeat::WorkerHeartbeatResponse;
use shared::messages::queue::{
    MasterFrameQueueAddRequest,
    MasterFrameQueueRemoveRequest,
    WorkerFrameQueueAddResponse,
    WorkerFrameQueueRemoveResponse,
};
use shared::messages::traits::IntoWebSocketMessage;
use shared::messages::{parse_websocket_message, receive_exact_message, WebSocketMessage};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, tungstenite, MaybeTlsStream, WebSocketStream};
use url::Url;

use crate::connection::event_dispatcher::MasterEventDispatcher;
use crate::rendering::queue::WorkerAutomaticQueue;
use crate::rendering::runner::BlenderJobRunner;


/// Worker instance. Manages the connection with the master server, receives requests
/// and performs the rendering as instructed.
pub struct Worker {
    ws_sink: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    ws_stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,

    sender_tx: UnboundedSender<Message>,
    sender_rx: UnboundedReceiver<Message>,

    receiver_tx: UnboundedSender<WebSocketMessage>,
    receiver_rx: UnboundedReceiver<WebSocketMessage>,
}

impl Worker {
    /// Connect to the master server
    pub async fn connect(master_server_url: Url) -> Result<Self> {
        let (stream, _) = connect_async(master_server_url).await.into_diagnostic()?;

        // Split the WebSocket stream and prepare async channels, but don't actually perform the handshake and other stuff yet.
        // See `run_forever` for running the worker.
        let (ws_write, ws_read) = stream.split();

        let (ws_sender_tx, ws_sender_rx) = futures_channel::mpsc::unbounded::<Message>();
        let (ws_receiver_tx, ws_receiver_rx) =
            futures_channel::mpsc::unbounded::<WebSocketMessage>();

        Ok(Self {
            ws_sink: ws_write,
            ws_stream: ws_read,
            sender_tx: ws_sender_tx,
            sender_rx: ws_sender_rx,
            receiver_tx: ws_receiver_tx,
            receiver_rx: ws_receiver_rx,
        })
    }

    /// This performs the initial handshake with the master server, then runs the worker,
    /// spawning several async tasks that receive, send and parse messages.
    ///
    /// Whenever a frame is queued, we render it and respond with the item finished event
    /// (but only one frame at a time).
    pub async fn run_forever(self, blender_runner: BlenderJobRunner) -> Result<()> {
        let sender_channel = Arc::new(self.sender_tx);
        let receiver_channel = Arc::new(Mutex::new(self.receiver_rx));

        let mut worker_connection_future_set: JoinSet<Result<()>> = JoinSet::new();

        let incoming_messages_handler =
            Self::forward_incoming_messages_through_channel(self.ws_stream, self.receiver_tx);
        worker_connection_future_set.spawn(incoming_messages_handler);

        let outgoing_messages_handler =
            Self::forward_queued_outgoing_messages_through_websocket(self.ws_sink, self.sender_rx);
        worker_connection_future_set.spawn(outgoing_messages_handler);

        let handshake_handle = worker_connection_future_set.spawn(Self::perform_handshake(
            sender_channel.clone(),
            receiver_channel.clone(),
        ));

        debug!("Waiting for handshake to complete.");
        worker_connection_future_set.join_next().await;
        if !handshake_handle.is_finished() {
            return Err(miette!(
                "BUG: Incorrect task completed (expected handshake)."
            ));
        }

        let event_dispatcher = MasterEventDispatcher::new(
            Arc::try_unwrap(receiver_channel)
                .map_err(|_| {
                    miette!(
                        "BUG: Failed to unwrap receiver channel Arc (is handshake still running?!)"
                    )
                })?
                .into_inner(),
        )
        .await;

        let event_dispatcher_arc = Arc::new(event_dispatcher);

        worker_connection_future_set.spawn(Self::respond_to_heartbeats(
            event_dispatcher_arc.clone(),
            sender_channel.clone(),
        ));

        worker_connection_future_set.spawn(Self::manage_incoming_messages(
            blender_runner,
            event_dispatcher_arc,
            sender_channel.clone(),
        ));

        info!("Connection fully established and async tasks are running.");

        while !worker_connection_future_set.is_empty() {
            worker_connection_future_set.join_next().await;
        }

        Ok(())
    }

    /// Performs our internal WebSocket handshake
    /// (master handshake request, worker response, master acknowledgement).
    async fn perform_handshake(
        sender_channel: Arc<UnboundedSender<Message>>,
        receiver_channel: Arc<Mutex<UnboundedReceiver<WebSocketMessage>>>,
    ) -> Result<()> {
        info!("Waiting for handshake request from master server.");

        let handshake_request = {
            let mut locked_receiver = receiver_channel.lock().await;
            receive_exact_message::<MasterHandshakeRequest>(&mut locked_receiver)
                .await
                .wrap_err_with(|| miette!("Invalid message: expected master handshake request."))?
        };

        info!(
            "Got handshake request from master server (server_version={}), sending response.",
            handshake_request.server_version
        );

        WorkerHandshakeResponse::new("1.0.0")
            .into_ws_message()
            .send(&sender_channel)
            .wrap_err_with(|| miette!("Could not send handshake response."))?;

        debug!("Sent handshake response, waiting for acknowledgement.");

        let handshake_ack = {
            let mut locked_receiver = receiver_channel.lock().await;
            receive_exact_message::<MasterHandshakeAcknowledgement>(&mut locked_receiver)
                .await
                .wrap_err_with(|| {
                    miette!("Invalid message: expected master handshake acknowledgement.")
                })?
        };

        if !handshake_ack.ok {
            return Err(miette!(
                "Server rejected worker (acknowledgement not ok)."
            ));
        }

        info!("Handshake finished, server fully connected.");
        Ok(())
    }

    /// Read from the unbounded async sending channel and forward the messages
    /// through the WebSocket connection with the master server.
    ///
    /// Runs as long as the channel can be read from or as long as the WebSocket sink can be written to.
    async fn forward_queued_outgoing_messages_through_websocket(
        mut ws_sink: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        mut message_send_queue: UnboundedReceiver<Message>,
    ) -> Result<()> {
        debug!("Running task loop: forwarding messages from send queue through WebSocket.");

        loop {
            let next_outgoing_message = message_send_queue
                .next()
                .await
                .ok_or_else(|| miette!("Can't get queued outgoing message from channel!"))?;

            ws_sink
                .send(next_outgoing_message)
                .await
                .into_diagnostic()
                .wrap_err_with(|| miette!("Could not send queued outgoing message, sink failed."))?;
        }
    }

    /// Read the WebSocket stream, parse messages and forward them through another unbounded async channel.
    /// That channel is either in the hands of the handshaking method or the event dispatcher.
    ///
    /// Runs as long as `stream` can be read from or until an error happens while parsing the messages.
    async fn forward_incoming_messages_through_channel(
        ws_stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        message_sender_channel: UnboundedSender<WebSocketMessage>,
    ) -> Result<()> {
        debug!("Running task loop: receiving, parsing and forwarding incoming messages.");

        ws_stream
            .try_for_each(|ws_message| async {
                let message = match parse_websocket_message(ws_message) {
                    Ok(optional_message) => match optional_message {
                        Some(concrete_message) => concrete_message,
                        None => {
                            return Ok(());
                        }
                    },
                    Err(error) => {
                        return Err(tungstenite::Error::Io(io::Error::new(
                            ErrorKind::Other,
                            error.to_string(),
                        )));
                    }
                };

                let channel_send_result = message_sender_channel.unbounded_send(message);
                if let Err(error) = channel_send_result {
                    return Err(tungstenite::Error::Io(io::Error::new(
                        ErrorKind::Other,
                        error.to_string(),
                    )));
                }

                Ok(())
            })
            .await
            .into_diagnostic()
            .wrap_err_with(|| miette!("Failed to receive incoming WebSocket message."))
    }

    /// Listen for heartbeat requests from the master server and respond to them.
    ///
    /// Runs as long as the async sender channel can be written to.
    async fn respond_to_heartbeats(
        event_dispatcher: Arc<MasterEventDispatcher>,
        sender_channel: Arc<UnboundedSender<Message>>,
    ) -> Result<()> {
        debug!("Running task loop: responding to heartbeats.");

        let mut heartbeat_request_receiver = event_dispatcher.heartbeat_request_receiver();

        loop {
            heartbeat_request_receiver
                .recv()
                .await
                .into_diagnostic()
                .wrap_err_with(|| miette!("Could not receive heartbeat request through channel."))?;

            info!("Server sent heartbeat request, responding.");

            WorkerHeartbeatResponse::new()
                .into_ws_message()
                .send(&sender_channel)
                .wrap_err_with(|| {
                    miette!("Could not send heartbeat response through sender channel.")
                })?;
        }
    }

    /// Waits for incoming requests and reacts to them
    /// (e.g. a "add frame to queue" request will update our frame queue and,
    /// if previously idle, start rendering).
    ///
    /// Runs as long as the event dispatcher's `Receiver`s can be read from.
    async fn manage_incoming_messages(
        blender_runner: BlenderJobRunner,
        event_dispatcher: Arc<MasterEventDispatcher>,
        sender_channel: Arc<UnboundedSender<Message>>,
    ) -> Result<()> {
        debug!("Running task loop: handling incoming messages.");

        let frame_queue = WorkerAutomaticQueue::new(blender_runner, sender_channel.clone());
        frame_queue.start().await;

        let mut queue_add_request_receiver = event_dispatcher.frame_queue_add_request_receiver();
        let mut queue_remove_request_receiver =
            event_dispatcher.frame_queue_remove_request_receiver();

        loop {
            tokio::select! {
                queue_add_request = queue_add_request_receiver.recv() => {
                    let queue_add_request: MasterFrameQueueAddRequest = queue_add_request.into_diagnostic()?;

                    debug!(
                        "Received: Frame queue add request, frame {}",
                        queue_add_request.frame_index
                    );

                    frame_queue.queue_frame(
                        queue_add_request.job,
                        queue_add_request.frame_index
                    ).await;

                    WorkerFrameQueueAddResponse::new_ok(queue_add_request.message_request_id)
                        .into_ws_message()
                        .send(&sender_channel)
                        .wrap_err_with(|| miette!("Could not send frame add response."))?;
                },
                queue_remove_request = queue_remove_request_receiver.recv() => {
                    let queue_remove_request: MasterFrameQueueRemoveRequest = queue_remove_request.into_diagnostic()?;

                    debug!(
                        "Received: Frame queue remove request, frame {}",
                        queue_remove_request.frame_index
                    );

                    let remove_result = frame_queue.unqueue_frame(
                        queue_remove_request.job_name,
                        queue_remove_request.frame_index
                    ).await;

                    debug!("Frame removal result: {remove_result:?}, responding.");

                    WorkerFrameQueueRemoveResponse::new_with_result(
                        queue_remove_request.message_request_id,
                        remove_result
                    )
                        .into_ws_message()
                        .send(&sender_channel)
                        .wrap_err_with(|| miette!("Could not send frame queue removal response."))?;
                }
            }
        }
    }
}
