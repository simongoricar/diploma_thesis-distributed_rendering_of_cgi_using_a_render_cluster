use std::sync::Arc;
use std::time::Duration;

use futures_util::stream::SplitStream;
use futures_util::StreamExt;
use log::{info, trace, warn};
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
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio_tungstenite::WebSocketStream;

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

static RECEIVER_BROADCAST_CHANNEL_SIZE: usize = 512;

impl WorkerReceiver {
    /// Initialize a new `WorkerEventDispatcher`, consuming the WebSocket connection's async receiver channel.
    pub fn new(
        websocket_stream: SplitStream<WebSocketStream<TcpStream>>,
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
            websocket_stream,
            senders.clone(),
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
        mut websocket_stream: SplitStream<WebSocketStream<TcpStream>>,
        broadcast_senders: BroadcastSenders,
        global_cancellation_token: CancellationToken,
    ) -> Result<()> {
        info!("WorkerReceiver: Running WebSocket stream reading and broadcasting loop.");

        loop {
            let next_message_result =
                tokio::time::timeout(Duration::from_secs(2), websocket_stream.next()).await;

            if global_cancellation_token.cancelled() {
                trace!("WorkerReceiver: Stopping (cluster stopping).");
                break;
            }

            let raw_message = match next_message_result {
                Ok(potential_message) => potential_message
                    .ok_or_else(|| miette!("Could not read from WebSocket stream - None."))?
                    .into_diagnostic()
                    .wrap_err_with(|| miette!("Errored while receiving from WebSocket stream."))?,
                // Simply wait two more seconds for the next message
                // (this Err happens when we time out on waiting for the next message).
                Err(_) => continue,
            };

            let parsed_message = {
                let optional_message = parse_websocket_message(raw_message)
                    .wrap_err_with(|| miette!("Could not parse incoming WebSocket message."))?;

                if let Some(message) = optional_message {
                    message
                } else {
                    // Simply not a WS message we should care about (Ping/Pong/Frame/etc.).
                    continue;
                }
            };

            // We ignore the errors when `.send`-ing because the only reason `send` can fail
            // is when there are no receivers. We don't want to propagate such an error as it
            // isn't really an error, it's just that no-one will see that message (as no one is subscribed).
            match parsed_message {
                WebSocketMessage::WorkerHandshakeResponse(response) => {
                    let _ = broadcast_senders.handshake_response.send(response);
                }
                WebSocketMessage::WorkerFrameQueueAddResponse(response) => {
                    let _ = broadcast_senders.queue_item_add_response.send(response);
                }
                WebSocketMessage::WorkerFrameQueueRemoveResponse(response) => {
                    let _ = broadcast_senders.queue_item_remove_response.send(response);
                }
                WebSocketMessage::WorkerFrameQueueItemRenderingEvent(event) => {
                    let _ = broadcast_senders.queue_item_rendering_event.send(event);
                }
                WebSocketMessage::WorkerFrameQueueItemFinishedEvent(event) => {
                    let _ = broadcast_senders.queue_item_finished_event.send(event);
                }
                WebSocketMessage::WorkerHeartbeatResponse(response) => {
                    let _ = broadcast_senders.heartbeat_response.send(response);
                }
                WebSocketMessage::WorkerJobFinishedResponse(response) => {
                    let _ = broadcast_senders.job_finished_response.send(response);
                }
                _ => {
                    warn!(
                        "WorkerReceiver: Unexpected (but valid) incoming WebSocket message: {}",
                        parsed_message.type_name()
                    );
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
        let timeout = timeout.unwrap_or(Duration::from_secs(5));

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
        let timeout = timeout.unwrap_or(Duration::from_secs(5));

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
    /// or when the `timeout` is reached, whichever is sooner. The default timeout is 5 seconds.
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
    /// or when the `timeout` is reached, whichever is sooner. The default timeout is 5 seconds.
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
