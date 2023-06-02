pub mod event_dispatcher;
pub mod queue;
pub mod requester;

use std::io;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt, TryStreamExt};
use log::debug;
use miette::{miette, Context, IntoDiagnostic, Result};
use shared::jobs::BlenderJob;
use shared::logger::Logger;
use shared::messages::handshake::{
    MasterHandshakeAcknowledgement,
    MasterHandshakeRequest,
    WorkerHandshakeResponse,
};
use shared::messages::heartbeat::MasterHeartbeatRequest;
use shared::messages::queue::{FrameQueueAddResult, WorkerFrameQueueItemFinishedEvent};
use shared::messages::traits::IntoWebSocketMessage;
use shared::messages::{parse_websocket_message, receive_exact_message, WebSocketMessage};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{accept_async, tungstenite, WebSocketStream};

use crate::cluster::state::ClusterManagerState;
use crate::connection::event_dispatcher::WorkerEventDispatcher;
use crate::connection::queue::{FrameOnWorker, WorkerQueue};
use crate::connection::requester::WorkerRequester;

/// State of the WebSocket connection with the worker.
pub enum WorkerConnectionState {
    PendingHandshake,
    Connected,
}

/// Worker abstraction from the viewpoint of the master server.
pub struct Worker {
    /// IP address and port of the worker.
    pub address: SocketAddr,

    /// Logger instance (so we can display logs with IP address, port and other context).
    pub logger: Arc<Logger>,

    /// Reference to the `ClusterManager`'s state.
    pub cluster_state: Arc<ClusterManagerState>,

    /// Unbounded async sender channel that can be used to send WebSocket messages to the worker.
    pub sender_channel: Arc<UnboundedSender<Message>>,

    /// Status of the WebSocket connection with the worker.
    pub connection_state: WorkerConnectionState,

    /// Event dispatcher for this worker. After the handshake is complete, this handles
    /// all of the incoming requests/responses/events and distributes them through
    /// distinct async channels.  
    pub event_dispatcher: Arc<WorkerEventDispatcher>,

    pub requester: Arc<WorkerRequester>,

    /// Internal queue representation for this worker.
    /// Likely to be slightly off sync with the actual worker (as WebSockets introduce minimal latency).
    pub queue: Arc<Mutex<WorkerQueue>>,

    /// `JoinSet` that contains all the async tasks this worker has running.
    pub connection_tasks: JoinSet<Result<()>>,
}

impl Worker {
    /// Given an established incoming `TcpStream`, turn the connection into
    /// a WebSocket connection, perform our internal handshake and spawn tasks to run the connection
    /// with this client and handle messages.
    pub async fn new_with_accept_and_handshake(
        stream: TcpStream,
        address: SocketAddr,
        cluster_state: Arc<ClusterManagerState>,
    ) -> Result<Self> {
        let queue = Arc::new(Mutex::new(WorkerQueue::new()));

        let (logger, message_sender_channel, event_dispatcher, requester, connection_tasks) =
            Self::accept_ws_stream_and_initialize_tasks(
                stream,
                queue.clone(),
                cluster_state.clone(),
            )
            .await?;

        Ok(Self {
            address,
            logger,
            cluster_state,
            sender_channel: message_sender_channel,
            connection_state: WorkerConnectionState::Connected,
            event_dispatcher,
            requester,
            queue,
            connection_tasks,
        })
    }

    /*
     * Public methods
     */

    /// Whether the worker has an empty queue
    /// (as far as the representation on the master server is currently concerned, this might have some latency).
    pub async fn has_empty_queue(&self) -> bool {
        self.queue.lock().await.is_empty()
    }

    /// Queue a frame onto the worker. This sends a WebSocket message and waits for the worker response.
    pub async fn queue_frame(&self, job: BlenderJob, frame_index: usize) -> Result<()> {
        let add_result = self
            .requester
            .frame_queue_add_item(job.clone(), frame_index)
            .await?;

        match add_result {
            FrameQueueAddResult::AddedToQueue => {
                self.queue
                    .lock()
                    .await
                    .add(FrameOnWorker::new(job, frame_index));
                Ok(())
            }
            FrameQueueAddResult::Errored { reason } => Err(miette!(
                "Errored on worker when trying to add frame to queue: {}",
                reason
            )),
        }
    }

    /*
     * Private WebSocket accepting, handshaking and other connection code.
     */

    /// Given a `TcpStream`, turn it into a proper WebSocket connection, perform the initial handshake
    /// and spawn tasks to run this connection, including handling incoming requests/responses/events.
    async fn accept_ws_stream_and_initialize_tasks(
        stream: TcpStream,
        queue: Arc<Mutex<WorkerQueue>>,
        cluster_state: Arc<ClusterManagerState>,
    ) -> Result<(
        Arc<Logger>,
        Arc<UnboundedSender<Message>>,
        Arc<WorkerEventDispatcher>,
        Arc<WorkerRequester>,
        JoinSet<Result<()>>,
    )> {
        // Initialize logger that will be distributed across this worker instance.
        let address = stream.peer_addr().into_diagnostic()?;
        let logger = Arc::new(Logger::new(format!(
            "[worker|{}:{}]",
            address.ip(),
            address.port()
        )));

        // Upgrades TcpStream to a WebSocket connection.
        logger.debug("Accepting TcpStream.");
        let ws_stream = accept_async(stream)
            .await
            .into_diagnostic()
            .wrap_err_with(|| miette!("Could not accept TcpStream."))?;
        logger.debug("TcpStream accepted.");

        // Splits the stream and initializes unbounded message channels.

        let (ws_sink, ws_stream) = ws_stream.split();

        // To send messages through the WebSocket, send a Message instance through this unbounded channel.
        let (ws_sender_tx, ws_sender_rx) = futures_channel::mpsc::unbounded::<Message>();
        // To get incoming messages, read this channel.
        let (ws_receiver_tx, ws_receiver_rx) =
            futures_channel::mpsc::unbounded::<WebSocketMessage>();

        let sender_channel = Arc::new(ws_sender_tx);
        let receiver_channel = Arc::new(Mutex::new(ws_receiver_rx));

        // The multiple async tasks that run this connection live on this `JoinSet`.
        // Use this `JoinSet` to cancel or join all of the worker tasks.
        let mut worker_connection_future_set: JoinSet<Result<()>> = JoinSet::new();

        // Spawn two tasks:
        // - one that receives, parses and forwards the incoming WebSocket messages through a receiver channel and
        // - one that reads the sender channel and forwards the messages through the WebSocket connection.
        let incoming_messages_handler = Self::forward_incoming_messages_through_channel(
            logger.clone(),
            ws_stream,
            ws_receiver_tx,
        );
        worker_connection_future_set.spawn(incoming_messages_handler);

        let outgoing_messages_handler = Self::forward_queued_outgoing_messages_through_websocket(
            logger.clone(),
            ws_sink,
            ws_sender_rx,
        );
        worker_connection_future_set.spawn(outgoing_messages_handler);

        // Perform our internal WebSocket handshake that exchanges, among other things, server and worker versions.
        let handshake_handle = worker_connection_future_set.spawn(Self::perform_handshake(
            logger.clone(),
            sender_channel.clone(),
            receiver_channel.clone(),
        ));

        // Wait for handshake to complete, then initialize the event dispatcher
        // that will consume the incoming WebSocket traffic for this worker from now on.
        // *All incoming messages can now be received only through this multiplexing event dispatcher.*
        logger.debug("Waiting for handshake to complete.");
        worker_connection_future_set.join_next().await;
        if !handshake_handle.is_finished() {
            return Err(miette!(
                "BUG: Incorrect task completed (expected handshake)."
            ));
        }

        let event_dispatcher = WorkerEventDispatcher::new(
            Arc::try_unwrap(receiver_channel)
                .map_err(|_| {
                    miette!(
                        "BUG: failed to unwrap receiver channel Arc (is handshake still running?!)"
                    )
                })?
                .into_inner(),
        )
        .await;
        let event_dispatcher_arc = Arc::new(event_dispatcher);

        let requester = WorkerRequester::new(
            sender_channel.clone(),
            event_dispatcher_arc.clone(),
        );
        let requester_arc = Arc::new(requester);

        // Finally, spawn two final tasks:
        // - one that maintains the heartbeat with the worker (it sends a heartbeat request every ~10 seconds and waits for the response) and
        // - one that, finally, actually manages the incoming messages - things like requests/responses/notifications
        //   and updates the master server's internal worker state if necessary.
        worker_connection_future_set.spawn(Self::maintain_heartbeat(
            logger.clone(),
            event_dispatcher_arc.clone(),
            sender_channel.clone(),
        ));

        worker_connection_future_set.spawn(Self::manage_incoming_messages(
            logger.clone(),
            event_dispatcher_arc.clone(),
            queue,
            cluster_state,
        ));

        logger.info("Worker connection fully established.");

        Ok((
            logger,
            sender_channel,
            event_dispatcher_arc,
            requester_arc,
            worker_connection_future_set,
        ))
    }

    /// Perform our internal handshake over the given WebSocket.
    /// This handshake consists of three messages:
    ///  1. The master server must send a "handshake request".
    ///  2. The worker must respond with a "handshake response".
    ///  3. The master server must confirm with a "handshake response acknowledgement".
    ///
    /// After these three steps, the connection is considered to be established.
    async fn perform_handshake(
        logger: Arc<Logger>,
        sender_channel: Arc<UnboundedSender<Message>>,
        receiver_channel: Arc<Mutex<UnboundedReceiver<WebSocketMessage>>>,
    ) -> Result<()> {
        logger.debug("Sending handshake request.");

        MasterHandshakeRequest::new("1.0.0")
            .into_ws_message()
            .send(&sender_channel)
            .wrap_err_with(|| miette!("Could not send initial handshake request."))?;

        let response = {
            let mut locked_receiver = receiver_channel.lock().await;
            receive_exact_message::<WorkerHandshakeResponse>(&mut locked_receiver)
                .await
                .wrap_err_with(|| miette!("Invalid message: expected worker handshake response!"))?
        };

        logger.info(format!(
            "Got handshake response from worker. worker_version={}, sending acknowledgement.",
            response.worker_version,
        ));

        MasterHandshakeAcknowledgement::new(true)
            .into_ws_message()
            .send(&sender_channel)
            .wrap_err_with(|| miette!("Could not send handshake acknowledgement."))?;

        logger.info("Handshake has been completed.");

        Ok(())
    }

    /// Read the WebSocket stream, parse messages and forward them through another unbounded async channel.
    /// That channel is either in the hands of the handshaking method or the event dispatcher.
    ///
    /// Runs as long as `stream` can be read from or until an error happens while parsing the messages.
    async fn forward_incoming_messages_through_channel(
        logger: Arc<Logger>,
        stream: SplitStream<WebSocketStream<TcpStream>>,
        message_sender_channel: UnboundedSender<WebSocketMessage>,
    ) -> Result<()> {
        logger.debug("Running task loop: receiving, parsing and forwarding incoming messages.");

        stream
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

    /// Read from the unbounded async sending channel and forward the messages
    /// through the WebSocket connection with the worker.
    ///
    /// Runs as long as the channel can be read from or as long as the WebSocket sink can be written to.
    async fn forward_queued_outgoing_messages_through_websocket(
        logger: Arc<Logger>,
        mut ws_sink: SplitSink<WebSocketStream<TcpStream>, Message>,
        mut message_send_queue_receiver: UnboundedReceiver<Message>,
    ) -> Result<()> {
        logger.debug(
            "Running task loop: forwarding messages from channel through WebSocket connection.",
        );

        loop {
            let next_outgoing_message = message_send_queue_receiver
                .next()
                .await
                .ok_or_else(|| miette!("Can't get outgoing message from channel!"))?;

            ws_sink
                .send(next_outgoing_message)
                .await
                .into_diagnostic()
                .wrap_err_with(|| miette!("Could not send outgoing message, sink failed."))?;
        }
    }

    /// Waits for incoming messages and reacts according to their contents
    /// (e.g. a "frame finished" event will update our internal queue to reflect that).
    async fn manage_incoming_messages(
        logger: Arc<Logger>,
        event_dispatcher: Arc<WorkerEventDispatcher>,
        queue: Arc<Mutex<WorkerQueue>>,
        cluster_state: Arc<ClusterManagerState>,
    ) -> Result<()> {
        logger.debug("Running task loop: handling incoming messages.");

        let mut queue_item_finished_receiver =
            event_dispatcher.frame_queue_item_finished_event_receiver();

        loop {
            tokio::select! {
                notification = queue_item_finished_receiver.recv() => {
                    let notification: WorkerFrameQueueItemFinishedEvent = notification.into_diagnostic()?;

                    debug!(
                        "Received: Queue item finished, frame: {}",
                        notification.frame_index
                    );

                    Self::mark_frame_as_finished(
                        notification.job_name,
                        notification.frame_index,
                        logger.clone(),
                        queue.clone(),
                        cluster_state.clone()
                    ).await?;
                }
            }
        }
    }

    /// Maintain a heartbeat with the worker. Every 10 seconds, a heartbeat request is sent
    /// and the worker is expected to respond to it in at most 5 seconds.
    ///
    /// Runs as long as the async sender channel can be written to
    /// or until the worker doesn't respond to a heartbeat.
    async fn maintain_heartbeat(
        logger: Arc<Logger>,
        event_dispatcher: Arc<WorkerEventDispatcher>,
        sender_channel: Arc<UnboundedSender<Message>>,
    ) -> Result<()> {
        logger.debug("Running task loop: heartbeats.");

        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;

            logger.debug("Sending heartbeat request to worker.");
            MasterHeartbeatRequest::new()
                .into_ws_message()
                .send(&sender_channel)
                .wrap_err_with(|| miette!("Could not send heartbeat request."))?;

            let time_heartbeat_wait_start = Instant::now();
            event_dispatcher
                .wait_for_heartbeat_response(None)
                .await
                .wrap_err_with(|| miette!("Worker did not respond to heartbeat."))?;

            let time_heartbeat_wait_latency = time_heartbeat_wait_start.elapsed();
            logger.debug(format!(
                "Worker responded to heartbeat request in {:.4} seconds.",
                time_heartbeat_wait_latency.as_secs_f64(),
            ));
        }
    }

    /*
     * Internal state management methods
     */

    /// Updates the internal `Worker` and slightly more global `ClusterManagerState` state
    /// to reflect the worker having finished its queued frame.
    async fn mark_frame_as_finished(
        job_name: String,
        frame_index: usize,
        logger: Arc<Logger>,
        queue: Arc<Mutex<WorkerQueue>>,
        cluster_state: Arc<ClusterManagerState>,
    ) -> Result<()> {
        {
            let mut locked_queue = queue.lock().await;
            logger.trace("Removing frame from master's internal worker state.");
            locked_queue.remove(job_name, frame_index)?
        }

        {
            logger.trace("Marking frame as finished in master's full state.");
            cluster_state.mark_frame_as_finished(frame_index).await?;
        }

        Ok(())
    }
}
