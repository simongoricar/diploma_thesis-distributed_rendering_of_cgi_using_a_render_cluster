pub mod event_dispatcher;

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use log::{debug, info, trace};
use miette::{miette, Context, IntoDiagnostic, Result};
use shared::cancellation::CancellationToken;
use shared::messages::handshake::{
    MasterHandshakeAcknowledgement,
    MasterHandshakeRequest,
    WorkerHandshakeResponse,
};
use shared::messages::heartbeat::WorkerHeartbeatResponse;
use shared::messages::job::{
    MasterJobFinishedRequest,
    MasterJobStartedEvent,
    WorkerJobFinishedResponse,
};
use shared::messages::queue::{
    MasterFrameQueueAddRequest,
    MasterFrameQueueRemoveRequest,
    WorkerFrameQueueAddResponse,
    WorkerFrameQueueRemoveResponse,
};
use shared::messages::traits::IntoWebSocketMessage;
use shared::messages::{
    parse_websocket_message,
    receive_exact_message,
    OutgoingMessage,
    WebSocketMessage,
};
use shared::results::worker_trace::WorkerTraceBuilder;
use tokio::net::TcpStream;
use tokio::sync::{watch, Mutex};
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use url::Url;

use crate::connection::event_dispatcher::MasterEventDispatcher;
use crate::rendering::queue::WorkerAutomaticQueue;
use crate::rendering::runner::BlenderJobRunner;

/// Worker instance. Manages the connection with the master server, receives requests
/// and performs the rendering as instructed.
pub struct Worker {}

impl Worker {
    /// This connects and performs the initial handshake with the master server, then runs the worker,
    /// spawning several async tasks that receive, send and parse messages.
    ///
    /// Whenever a frame is queued, we render it and respond with the item finished event
    /// (but only one frame at a time).
    pub async fn connect_and_run_to_job_completion(
        master_server_url: Url,
        blender_runner: BlenderJobRunner,
    ) -> Result<()> {
        let (stream, _) = connect_async(master_server_url).await.into_diagnostic()?;

        // Split the WebSocket stream and prepare async channels, but don't actually perform the handshake and other stuff yet.
        // See `run_forever` for running the worker.
        let (ws_write, ws_read) = stream.split();

        let (ws_sender_tx, ws_sender_rx) = futures_channel::mpsc::unbounded::<OutgoingMessage>();
        let (ws_receiver_tx, ws_receiver_rx) =
            futures_channel::mpsc::unbounded::<WebSocketMessage>();

        let cancellation_token = CancellationToken::new();
        // TODO Integrate into job runner
        let trace_builder = WorkerTraceBuilder::new_empty();

        let sender_channel = Arc::new(ws_sender_tx);
        let receiver_channel = Arc::new(Mutex::new(ws_receiver_rx));


        let mut worker_connection_future_set: JoinSet<Result<()>> = JoinSet::new();


        let incoming_messages_handler = Self::forward_incoming_messages_through_channel(
            ws_read,
            ws_receiver_tx,
            cancellation_token.clone(),
        );
        worker_connection_future_set.spawn(incoming_messages_handler);


        let (outgoing_sender_status_watcher_tx, outgoing_sender_status_watcher_rx) =
            watch::channel(true);

        let outgoing_messages_handler = Self::forward_queued_outgoing_messages_through_websocket(
            ws_write,
            ws_sender_rx,
            outgoing_sender_status_watcher_tx,
            cancellation_token.clone(),
        );
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
            cancellation_token.clone(),
        )
        .await;

        let event_dispatcher_arc = Arc::new(event_dispatcher);


        worker_connection_future_set.spawn(Self::respond_to_heartbeats(
            event_dispatcher_arc.clone(),
            sender_channel.clone(),
            cancellation_token.clone(),
        ));

        worker_connection_future_set.spawn(Self::manage_incoming_messages(
            blender_runner,
            event_dispatcher_arc.clone(),
            sender_channel.clone(),
            trace_builder,
            outgoing_sender_status_watcher_rx,
            cancellation_token.clone(),
        ));

        info!("Connection fully established and async tasks are running.");


        while !worker_connection_future_set.is_empty() {
            worker_connection_future_set.join_next().await;
        }

        let event_dispatcher = Arc::try_unwrap(event_dispatcher_arc)
            .map_err(|_| miette!("Could not unwrap event_dispatcher Arc?!"))?;
        event_dispatcher.join().await?;

        Ok(())
    }

    /// Performs our internal WebSocket handshake
    /// (master handshake request, worker response, master acknowledgement).
    async fn perform_handshake(
        sender_channel: Arc<UnboundedSender<OutgoingMessage>>,
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
            .send_through_channel(&sender_channel)
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
        mut message_send_queue: UnboundedReceiver<OutgoingMessage>,
        self_status_watch_sender: watch::Sender<bool>,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        debug!("Running task loop: forwarding messages from send queue through WebSocket.");

        loop {
            if let Ok(outgoing_message) =
                tokio::time::timeout(Duration::from_secs(2), message_send_queue.next()).await
            {
                let outgoing_message = outgoing_message
                    .ok_or_else(|| miette!("Can't read outgoing messages from channel!"))?;

                match outgoing_message {
                    OutgoingMessage::TungsteniteMessage(raw_message) => {
                        ws_sink
                            .send(raw_message)
                            .await
                            .into_diagnostic()
                            .wrap_err_with(|| {
                                miette!("Could not send raw tungstenite message, sink failed.")
                            })?;
                    }
                    OutgoingMessage::WebSocketMessage(ws_message) => {
                        let raw_message = ws_message
                            .to_websocket_message()
                            .wrap_err_with(|| miette!("Could not serialize WebSocketMessage."))?;

                        ws_sink
                            .send(raw_message)
                            .await
                            .into_diagnostic()
                            .wrap_err_with(|| {
                                miette!("Could not send WebSocket message, sink failed.")
                            })?;
                    }
                    OutgoingMessage::CloseConnectionControlMessage => {
                        info!("Stopping outgoing messages sender (received control message).");
                        break;
                    }
                }
            }

            if cancellation_token.cancelled() {
                trace!("Stopping outgoing messages sender (worker stopping).");
                break;
            }
        }

        self_status_watch_sender
            .send(false)
            .into_diagnostic()
            .wrap_err_with(|| {
                miette!("Could not set outgoing messages sender's status to false.")
            })?;

        Ok(())
    }

    /// Read the WebSocket stream, parse messages and forward them through another unbounded async channel.
    /// That channel is either in the hands of the handshaking method or the event dispatcher.
    ///
    /// Runs as long as `stream` can be read from or until an error happens while parsing the messages.
    async fn forward_incoming_messages_through_channel(
        mut ws_stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        message_sender_channel: UnboundedSender<WebSocketMessage>,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        debug!("Running task loop: receiving, parsing and forwarding incoming messages.");

        loop {
            if let Ok(next_message) =
                tokio::time::timeout(Duration::from_secs(2), ws_stream.next()).await
            {
                let next_message = next_message
                    .ok_or_else(|| miette!("No next message?!"))?
                    .into_diagnostic()
                    .wrap_err_with(|| miette!("Could not read from WebSocket stream."))?;

                match parse_websocket_message(next_message) {
                    Ok(optional_message) => {
                        if let Some(message) = optional_message {
                            let channel_send_result = message_sender_channel.unbounded_send(message);
                            if let Err(error) = channel_send_result {
                                return Err(error).into_diagnostic().wrap_err_with(|| {
                                    miette!(
                                        "Failed to send parsed incoming message through channel."
                                    )
                                });
                            }
                        }
                    }
                    Err(error) => {
                        return Err(error)
                            .wrap_err_with(|| miette!("Errored while parsing WebSocket message."));
                    }
                }
            }

            if cancellation_token.cancelled() {
                trace!("Stopping incoming messages parser/forwarder (worker stopping).");
                break;
            }
        }

        Ok(())
    }

    /// Listen for heartbeat requests from the master server and respond to them.
    ///
    /// Runs as long as the async sender channel can be written to.
    async fn respond_to_heartbeats(
        event_dispatcher: Arc<MasterEventDispatcher>,
        sender_channel: Arc<UnboundedSender<OutgoingMessage>>,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        debug!("Running task loop: responding to heartbeats.");

        let mut heartbeat_request_receiver = event_dispatcher.heartbeat_request_receiver();

        loop {
            if let Ok(request) = tokio::time::timeout(
                Duration::from_secs(2),
                heartbeat_request_receiver.recv(),
            )
            .await
            {
                request.into_diagnostic().wrap_err_with(|| {
                    miette!("Could not receive heartbeat request from broadcast channel.")
                })?;

                info!("Server sent heartbeat request, responding.");

                WorkerHeartbeatResponse::new()
                    .into_ws_message()
                    .send_through_channel(&sender_channel)
                    .wrap_err_with(|| {
                        miette!("Could not send heartbeat response through sender channel.")
                    })?;
            }

            if cancellation_token.cancelled() {
                trace!("Stopping heartbeat responder task (worker stopping).");
                break;
            }
        }

        Ok(())
    }

    /// Waits for incoming requests and reacts to them
    /// (e.g. a "add frame to queue" request will update our frame queue and,
    /// if previously idle, start rendering).
    ///
    /// Runs as long as the event dispatcher's `Receiver`s can be read from.
    async fn manage_incoming_messages(
        blender_runner: BlenderJobRunner,
        event_dispatcher: Arc<MasterEventDispatcher>,
        sender_channel: Arc<UnboundedSender<OutgoingMessage>>,
        tracer: WorkerTraceBuilder,
        mut outgoing_sender_status_watch_receiver: watch::Receiver<bool>,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        debug!("Running task loop: handling incoming messages.");

        let frame_queue = WorkerAutomaticQueue::new(blender_runner, sender_channel.clone());
        frame_queue.start().await;

        let mut queue_add_request_receiver = event_dispatcher.frame_queue_add_request_receiver();
        let mut queue_remove_request_receiver =
            event_dispatcher.frame_queue_remove_request_receiver();
        let mut job_started_event_receiver = event_dispatcher.job_started_event_receiver();
        let mut job_finished_request_receiver = event_dispatcher.job_finished_request_receiver();

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

                    tracer.trace_new_frame_queued().await;

                    WorkerFrameQueueAddResponse::new_ok(queue_add_request.message_request_id)
                        .into_ws_message()
                        .send_through_channel(&sender_channel)
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

                    tracer.trace_frame_stolen_from_queue().await;

                    debug!("Frame removal result: {remove_result:?}, responding.");

                    WorkerFrameQueueRemoveResponse::new_with_result(
                        queue_remove_request.message_request_id,
                        remove_result
                    )
                        .into_ws_message()
                        .send_through_channel(&sender_channel)
                        .wrap_err_with(|| miette!("Could not send frame queue removal response."))?;
                },
                job_started_event = job_started_event_receiver.recv() => {
                    let _: MasterJobStartedEvent = job_started_event.into_diagnostic()?;

                    info!("Master server signalled job start.");
                    tracer.set_job_start_time(SystemTime::now()).await;
                },
                job_finished_request = job_finished_request_receiver.recv() => {
                    let request: MasterJobFinishedRequest = job_finished_request.into_diagnostic()?;

                    info!("Master server signalled job finish - sending trace and stopping.");
                    tracer.set_job_finish_time(SystemTime::now()).await;

                    let trace = tracer
                        .build()
                        .await
                        .wrap_err_with(|| miette!("Could not build worker trace."))?;

                    WorkerJobFinishedResponse::new(
                        request.message_request_id,
                        trace,
                    )
                        .into_ws_message()
                        .send_through_channel(&sender_channel)
                        .wrap_err_with(|| miette!("Could not send job finished response."))?;

                    // Send the final message through the outgoing channel.
                    sender_channel
                        .unbounded_send(OutgoingMessage::CloseConnectionControlMessage)
                        .into_diagnostic()
                        .wrap_err_with(|| miette!("Could not send final control message."))?;

                    let _ = outgoing_sender_status_watch_receiver.wait_for(|value| !(*value)).await;

                    // Cancel the entire worker set of tasks.
                    debug!("Stopping incoming messages manager (worker stopping).");
                    cancellation_token.cancel();

                    break;
                },
                _ = tokio::time::sleep(Duration::from_secs(2)) => {
                    if cancellation_token.cancelled() {
                        trace!("Stopping incoming messages manager (worker stopping).");
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}
