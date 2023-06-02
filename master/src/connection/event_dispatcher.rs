use std::sync::Arc;
use std::time::Duration;

use futures_channel::mpsc::UnboundedReceiver;
use futures_util::StreamExt;
use log::{info, trace, warn};
use miette::{miette, IntoDiagnostic};
use miette::{Context, Result};
use shared::cancellation::CancellationToken;
use shared::messages::heartbeat::WorkerHeartbeatResponse;
use shared::messages::queue::{
    WorkerFrameQueueAddResponse,
    WorkerFrameQueueItemFinishedEvent,
    WorkerFrameQueueItemRenderingEvent,
    WorkerFrameQueueRemoveResponse,
};
use shared::messages::traits::Message;
use shared::messages::WebSocketMessage;
use tokio::sync::broadcast;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::task::JoinHandle;

#[derive(Clone)]
struct EventSenders {
    /// Receives worker heartbeat responses.
    heartbeat_response: Arc<Sender<WorkerHeartbeatResponse>>,

    queue_item_add_response: Arc<Sender<WorkerFrameQueueAddResponse>>,

    queue_item_remove_response: Arc<Sender<WorkerFrameQueueRemoveResponse>>,

    queue_item_rendering_event: Arc<Sender<WorkerFrameQueueItemRenderingEvent>>,

    // Receives worker's queue item finished events.
    queue_item_finished_event: Arc<Sender<WorkerFrameQueueItemFinishedEvent>>,
}

/// Manages all incoming WebSocket messages for a worker.
pub struct WorkerEventDispatcher {
    senders: EventSenders,

    /// `JoinHandle` for the async task that is running this dispatcher.
    /// As long as this task is running, receivers from this dispatcher will receive incoming messages.
    task_join_handle: JoinHandle<()>,
}

impl WorkerEventDispatcher {
    /// Initialize a new `WorkerEventDispatcher`, consuming the WebSocket connection's async receiver channel.
    pub async fn new(
        websocket_receiver_channel: UnboundedReceiver<WebSocketMessage>,
        cluster_cancellation_token: CancellationToken,
    ) -> Self {
        let (heartbeat_tx, _) = broadcast::channel::<WorkerHeartbeatResponse>(512);
        let (queue_item_add_tx, _) = broadcast::channel::<WorkerFrameQueueAddResponse>(512);
        let (queue_item_remove_tx, _) = broadcast::channel::<WorkerFrameQueueRemoveResponse>(512);
        let (queue_item_rendering_tx, _) =
            broadcast::channel::<WorkerFrameQueueItemRenderingEvent>(512);
        let (queue_item_finished_tx, _) =
            broadcast::channel::<WorkerFrameQueueItemFinishedEvent>(512);

        let heartbeat_tx_arc = Arc::new(heartbeat_tx);
        let queue_item_add_tx_arc = Arc::new(queue_item_add_tx);
        let queue_item_remove_tx_arc = Arc::new(queue_item_remove_tx);
        let queue_item_rendering_tx_arc = Arc::new(queue_item_rendering_tx);
        let queue_item_finished_tx_arc = Arc::new(queue_item_finished_tx);

        let senders = EventSenders {
            heartbeat_response: heartbeat_tx_arc,
            queue_item_add_response: queue_item_add_tx_arc,
            queue_item_remove_response: queue_item_remove_tx_arc,
            queue_item_rendering_event: queue_item_rendering_tx_arc,
            queue_item_finished_event: queue_item_finished_tx_arc,
        };

        let dispatcher_join_handle = tokio::spawn(Self::run(
            senders.clone(),
            websocket_receiver_channel,
            cluster_cancellation_token,
        ));

        Self {
            senders,
            task_join_handle: dispatcher_join_handle,
        }
    }

    pub async fn join(self) -> Result<()> {
        self.task_join_handle.await.into_diagnostic()
    }

    /// Run the event dispatcher indefinitely, reading the incoming WebSocket messages
    /// and broadcasting them to subscribed `tokio::broadcast::Receiver`s.
    async fn run(
        senders: EventSenders,
        mut websocket_receiver_channel: UnboundedReceiver<WebSocketMessage>,
        cluster_cancellation_token: CancellationToken,
    ) {
        info!("WorkerEventDispatcher: Running event distribution loop.");

        loop {
            if cluster_cancellation_token.cancelled() {
                trace!("WorkerEventDispatcher: Stopping (cluster stopping).");
                break;
            }

            let next_message = match tokio::time::timeout(
                Duration::from_secs(2),
                websocket_receiver_channel.next(),
            )
            .await
            {
                Ok(optional_message) => match optional_message {
                    Some(message) => message,
                    None => {
                        if cluster_cancellation_token.cancelled() {
                            trace!("WorkerEventDispatcher: Stopping (cluster stopping).");
                            break;
                        }

                        warn!("WorkerEventDispatcher: Can't receive message from channel, aborting event dispatcher task.");
                        return;
                    }
                },
                Err(_) => {
                    continue;
                }
            };

            // We ignore the errors when `.send`-ing because the only reason `send` can fail
            // is when there are no receivers. We don't want to propagate such an error
            // (as it really isn't an error, it's just that no-one will see that message as no-one is subscribed).
            match next_message {
                WebSocketMessage::WorkerFrameQueueAddResponse(response) => {
                    let _ = senders.queue_item_add_response.send(response);
                }
                WebSocketMessage::WorkerFrameQueueRemoveResponse(response) => {
                    let _ = senders.queue_item_remove_response.send(response);
                }
                WebSocketMessage::WorkerFrameQueueItemRenderingEvent(event) => {
                    let _ = senders.queue_item_rendering_event.send(event);
                }
                WebSocketMessage::WorkerFrameQueueItemFinishedEvent(event) => {
                    let _ = senders.queue_item_finished_event.send(event);
                }
                WebSocketMessage::WorkerHeartbeatResponse(response) => {
                    let _ = senders.heartbeat_response.send(response);
                }
                _ => {
                    warn!(
                        "WorkerEventDispatcher: Unexpected (but valid) incoming WebSocket message: {}",
                        next_message.type_name()
                    );
                }
            }
        }
    }

    /*
     * Public event channel methods.
     */

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
