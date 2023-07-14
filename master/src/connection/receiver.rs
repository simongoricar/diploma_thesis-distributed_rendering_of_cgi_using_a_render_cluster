use std::sync::Arc;
use std::time::Duration;

use miette::{miette, IntoDiagnostic};
use miette::{Context, Result};
use shared::cancellation::CancellationToken;
use shared::messages::handshake::WorkerHandshakeResponse;
use shared::messages::heartbeat::WorkerHeartbeatResponse;
use shared::messages::job::WorkerJobFinishedResponse;
use shared::messages::queue::{
    WorkerFrameQueueAddResponse,
    WorkerFrameQueueItemFinishedEvent,
    WorkerFrameQueueItemRenderingEvent,
    WorkerFrameQueueRemoveResponse,
};
use shared::messages::traits::Message;
use shared::messages::{parse_websocket_message, WebSocketMessage};
use tokio::sync::broadcast;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::task::JoinHandle;

use crate::cluster::ReconnectableClientConnection;
use crate::connection::worker_logger::WorkerLogger;

const NEXT_MESSAGE_TASK_CHECK_INTERVAL: Duration = Duration::from_secs(1);
const RECEIVER_BROADCAST_CHANNEL_SIZE: usize = 512;
const DEFAULT_MESSAGE_WAIT_DURATION: Duration = Duration::from_secs(60);


#[derive(Clone)]
struct BroadcastSenders {
    handshake_response: Arc<Sender<WorkerHandshakeResponse>>,

    queue_item_add_response: Arc<Sender<WorkerFrameQueueAddResponse>>,

    queue_item_remove_response: Arc<Sender<WorkerFrameQueueRemoveResponse>>,

    queue_item_rendering_event: Arc<Sender<WorkerFrameQueueItemRenderingEvent>>,

    // Receives worker's queue item finished events.
    queue_item_finished_event: Arc<Sender<WorkerFrameQueueItemFinishedEvent>>,

    /// Receives worker heartbeat responses.
    heartbeat_response: Arc<Sender<WorkerHeartbeatResponse>>,

    job_finished_response: Arc<Sender<WorkerJobFinishedResponse>>,
}

/// Manages all incoming WebSocket messages for a worker.
pub struct WorkerReceiver {
    senders: BroadcastSenders,

    /// `JoinHandle` for the async task that is running this dispatcher.
    /// As long as this task is running, receivers from this dispatcher will receive incoming messages.
    task_join_handle: JoinHandle<Result<()>>,
}


impl WorkerReceiver {
    /// Initialize a new `WorkerEventDispatcher`, consuming the WebSocket connection's async receiver channel.
    pub fn new(
        connection: Arc<ReconnectableClientConnection>,
        logger: WorkerLogger,
        global_cancellation_token: CancellationToken,
    ) -> Self {
        // Initialize broadcast channels.
        let (handshake_response_tx, _) =
            broadcast::channel::<WorkerHandshakeResponse>(RECEIVER_BROADCAST_CHANNEL_SIZE);
        let (queue_item_add_tx, _) =
            broadcast::channel::<WorkerFrameQueueAddResponse>(RECEIVER_BROADCAST_CHANNEL_SIZE);
        let (queue_item_remove_tx, _) =
            broadcast::channel::<WorkerFrameQueueRemoveResponse>(RECEIVER_BROADCAST_CHANNEL_SIZE);
        let (queue_item_rendering_tx, _) = broadcast::channel::<WorkerFrameQueueItemRenderingEvent>(
            RECEIVER_BROADCAST_CHANNEL_SIZE,
        );
        let (queue_item_finished_tx, _) =
            broadcast::channel::<WorkerFrameQueueItemFinishedEvent>(RECEIVER_BROADCAST_CHANNEL_SIZE);
        let (heartbeat_response_tx, _) =
            broadcast::channel::<WorkerHeartbeatResponse>(RECEIVER_BROADCAST_CHANNEL_SIZE);
        let (job_finished_response_tx, _) =
            broadcast::channel::<WorkerJobFinishedResponse>(RECEIVER_BROADCAST_CHANNEL_SIZE);

        // Wrap broadcast channels in `Arc`s and construct a `BroadcastSenders` to house them.
        let handshake_response_tx_arc = Arc::new(handshake_response_tx);
        let queue_item_add_tx_arc = Arc::new(queue_item_add_tx);
        let queue_item_remove_tx_arc = Arc::new(queue_item_remove_tx);
        let queue_item_rendering_tx_arc = Arc::new(queue_item_rendering_tx);
        let queue_item_finished_tx_arc = Arc::new(queue_item_finished_tx);
        let heartbeat_tx_arc = Arc::new(heartbeat_response_tx);
        let job_finished_response_tx_arc = Arc::new(job_finished_response_tx);

        let senders = BroadcastSenders {
            handshake_response: handshake_response_tx_arc,
            queue_item_add_response: queue_item_add_tx_arc,
            queue_item_remove_response: queue_item_remove_tx_arc,
            queue_item_rendering_event: queue_item_rendering_tx_arc,
            queue_item_finished_event: queue_item_finished_tx_arc,
            heartbeat_response: heartbeat_tx_arc,
            job_finished_response: job_finished_response_tx_arc,
        };

        let dispatcher_join_handle = tokio::spawn(Self::run(
            connection,
            senders.clone(),
            logger,
            global_cancellation_token,
        ));

        Self {
            senders,
            task_join_handle: dispatcher_join_handle,
        }
    }

    pub async fn join(self) -> Result<()> {
        self.task_join_handle.await.into_diagnostic()?
    }

    /// Run the event dispatcher indefinitely, reading the incoming WebSocket messages
    /// and broadcasting them to subscribed `tokio::broadcast::Receiver`s.
    async fn run(
        connection: Arc<ReconnectableClientConnection>,
        broadcast_senders: BroadcastSenders,
        logger: WorkerLogger,
        global_cancellation_token: CancellationToken,
    ) -> Result<()> {
        logger.info("Starting task loop: parsing and broadcasting incoming messages.");

        loop {
            if global_cancellation_token.is_cancelled() {
                logger.debug("Stopping parsing and incoming message broadcasting loop.");
                break;
            }

            let next_message_result = tokio::time::timeout(
                NEXT_MESSAGE_TASK_CHECK_INTERVAL,
                connection.receive_message(),
            )
            .await;

            let raw_message = match next_message_result {
                Ok(potential_message) => potential_message.wrap_err_with(|| {
                    miette!("Failed to receive message from reconnectable client connection.")
                })?,
                // Simply wait two more seconds for the next message
                // (this Err happens when we time out on waiting for the next message).
                Err(_) => continue,
            };

            let parsed_message = match parse_websocket_message(raw_message)
                .wrap_err_with(|| miette!("Could not parse incoming WebSocket message."))?
            {
                Some(message) => message,
                // This is a message we should ignore (ping/pong/...)
                None => continue,
            };

            // We ignore the errors when `.send`-ing because the only reason `send` can fail
            // is when there are no receivers. We don't want to propagate such an error as it
            // isn't really an error, it's just that no-one will see that message (as no one is subscribed).
            match parsed_message {
                WebSocketMessage::WorkerHandshakeResponse(response) => {
                    match broadcast_senders.handshake_response.send(response) {
                        Ok(num_subscribers) => {
                            logger.debug(format!("Received message: worker handshake response ({num_subscribers} subscribers)."));
                        }
                        Err(_) => {
                            logger.debug(
                                "Received message: worker handshake response (no subscribers).",
                            );
                        }
                    }
                }
                WebSocketMessage::WorkerFrameQueueAddResponse(response) => {
                    match broadcast_senders.queue_item_add_response.send(response) {
                        Ok(num_subscribers) => {
                            logger.debug(format!("Received message: worker frame queue add response ({num_subscribers} subscribers)."));
                        }
                        Err(_) => {
                            logger.debug("Received message: worker frame queue add response (no subscribers).");
                        }
                    }
                }
                WebSocketMessage::WorkerFrameQueueRemoveResponse(response) => {
                    match broadcast_senders.queue_item_remove_response.send(response) {
                        Ok(num_subscribers) => {
                            logger.debug(format!("Received message: worker frame queue remove response ({num_subscribers} subscribers)."));
                        }
                        Err(_) => {
                            logger.debug("Received message: worker frame queue remove response (no subscribers).");
                        }
                    }
                }
                WebSocketMessage::WorkerFrameQueueItemRenderingEvent(event) => {
                    match broadcast_senders.queue_item_rendering_event.send(event) {
                        Ok(num_subscribers) => {
                            logger.debug(format!("Received message: worker frame queue item rendering event ({num_subscribers} subscribers)."));
                        }
                        Err(_) => {
                            logger.debug("Received message: worker frame queue item rendering event (no subscribers).");
                        }
                    }
                }
                WebSocketMessage::WorkerFrameQueueItemFinishedEvent(event) => {
                    match broadcast_senders.queue_item_finished_event.send(event) {
                        Ok(num_subscribers) => {
                            logger.debug(format!("Received message: worker frame queue item finished event ({num_subscribers} subscribers)."));
                        }
                        Err(_) => {
                            logger.debug("Received message: worker frame queue item finished event (no subscribers).");
                        }
                    }
                }
                WebSocketMessage::WorkerHeartbeatResponse(response) => {
                    match broadcast_senders.heartbeat_response.send(response) {
                        Ok(num_subscribers) => {
                            logger.debug(format!("Received message: worker heartbeat response ({num_subscribers} subscribers)."));
                        }
                        Err(_) => {
                            logger.debug(
                                "Received message: worker heartbeat response (no subscribers).",
                            );
                        }
                    }
                }
                WebSocketMessage::WorkerJobFinishedResponse(response) => {
                    match broadcast_senders.job_finished_response.send(response) {
                        Ok(num_subscribers) => {
                            logger.debug(format!("Received message: worker job finished response ({num_subscribers} subscribers)."));
                        }
                        Err(_) => {
                            logger.debug(
                                "Received message: worker job finished response (no subscribers).",
                            );
                        }
                    }
                }
                _ => {
                    logger.error(format!(
                        "Received unexpected (though valid) message with type: {}",
                        parsed_message.type_name()
                    ));
                }
            }
        }

        Ok(())
    }

    /*
     * Public event channel methods.
     */

    pub fn handshake_response_receiver(&self) -> Receiver<WorkerHandshakeResponse> {
        self.senders.handshake_response.subscribe()
    }

    /// Get a `Receiver` for future "frame queue item add response" worker messages.
    pub fn frame_queue_item_add_response_receiver(&self) -> Receiver<WorkerFrameQueueAddResponse> {
        self.senders.queue_item_add_response.subscribe()
    }

    /// Get a `Receiver` for future "frame queue item removal response" worker messages.
    pub fn frame_queue_item_remove_response_receiver(
        &self,
    ) -> Receiver<WorkerFrameQueueRemoveResponse> {
        self.senders.queue_item_remove_response.subscribe()
    }

    /// Get a `Receiver` for future "frame queue item is rendering" worker messages.
    pub fn frame_queue_item_rendering_event_receiver(
        &self,
    ) -> Receiver<WorkerFrameQueueItemRenderingEvent> {
        self.senders.queue_item_rendering_event.subscribe()
    }

    /// Get a `Receiver` for future "frame queue item is finished" worker messages.
    pub fn frame_queue_item_finished_event_receiver(
        &self,
    ) -> Receiver<WorkerFrameQueueItemFinishedEvent> {
        self.senders.queue_item_finished_event.subscribe()
    }

    /// Get a `Receiver` for future "heartbeat response" worker messages.
    pub fn heartbeat_response_receiver(&self) -> Receiver<WorkerHeartbeatResponse> {
        self.senders.heartbeat_response.subscribe()
    }

    /// Get a `Receiver` for future "job finished response" worker messages.
    pub fn job_finished_response_receiver(&self) -> Receiver<WorkerJobFinishedResponse> {
        self.senders.job_finished_response.subscribe()
    }

    /*
     * One-shot async event methods
     */

    /// Generic "wait for this message type" abstraction.
    pub async fn wait_for_message<R: Message + Clone>(
        &self,
        mut receiver: Receiver<R>,
        timeout: Option<Duration>,
    ) -> Result<R> {
        let timeout = timeout.unwrap_or(DEFAULT_MESSAGE_WAIT_DURATION);

        let receiver_future = async {
            receiver.recv().await.into_diagnostic().wrap_err_with(|| {
                miette!(
                    "Could not receive message through channel (expected {}).",
                    R::type_name()
                )
            })
        };

        let resolved_future = tokio::time::timeout(timeout, receiver_future).await;
        match resolved_future {
            Ok(result) => result,
            Err(_) => Err(miette!(
                "Timed out while waiting for message: {}",
                R::type_name()
            )),
        }
    }

    /// Generic "wait for this message with additional check" abstraction.
    /// Provide a `receiver` to receive the messages through and a closure that takes a reference
    /// to the message and returns a boolean indicating whether that message matches what you want.
    pub async fn wait_for_message_with_predicate<R: Message + Clone, F: Fn(&R) -> bool>(
        &self,
        mut receiver: Receiver<R>,
        timeout: Option<Duration>,
        predicate: F,
    ) -> Result<R> {
        let timeout = timeout.unwrap_or(DEFAULT_MESSAGE_WAIT_DURATION);

        // Keeps receiving given messages until the predicate matches.
        let predicated_receiver_future = async {
            loop {
                let next_message = receiver.recv().await.into_diagnostic().wrap_err_with(|| {
                    miette!(
                        "Could not receive message through channel (expected {})",
                        R::type_name()
                    )
                });

                match next_message {
                    Ok(message) => {
                        if predicate(&message) {
                            break Ok(message);
                        }
                    }
                    Err(error) => {
                        break Err(error);
                    }
                }
            }
        };

        let resolved_future = tokio::time::timeout(timeout, predicated_receiver_future).await;
        match resolved_future {
            Ok(result) => result,
            Err(_) => Err(miette!(
                "Timed out while waiting for predicated message: {}",
                R::type_name()
            )),
        }
    }

    pub async fn wait_for_handshake_response(
        &self,
        timeout: Option<Duration>,
    ) -> Result<WorkerHandshakeResponse> {
        self.wait_for_message(self.handshake_response_receiver(), timeout)
            .await
    }

    pub async fn wait_for_frame_queue_add_item_response(
        &self,
        timeout: Option<Duration>,
    ) -> Result<WorkerFrameQueueAddResponse> {
        self.wait_for_message(
            self.frame_queue_item_add_response_receiver(),
            timeout,
        )
        .await
    }

    /// One-shot event method that completes when either some worker's queue item has been finished
    /// or when the `timeout` is reached, whichever is sooner. The default timeout is 10 seconds.
    ///
    /// If timed out, `Err` is returned.
    pub async fn wait_for_queue_item_finished(
        &self,
        timeout: Option<Duration>,
    ) -> Result<WorkerFrameQueueItemFinishedEvent> {
        self.wait_for_message(
            self.frame_queue_item_finished_event_receiver(),
            timeout,
        )
        .await
    }

    /// One-shot event method that completes when the worker sends the next heartbeat response
    /// or when the `timeout` is reached, whichever is sooner. The default timeout is 10 seconds.
    ///
    /// If timed out, `Err` is returned.
    pub async fn wait_for_heartbeat_response(
        &self,
        timeout: Option<Duration>,
    ) -> Result<WorkerHeartbeatResponse> {
        self.wait_for_message(self.heartbeat_response_receiver(), timeout)
            .await
    }
}
