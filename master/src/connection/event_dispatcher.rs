use std::sync::Arc;
use std::time::Duration;

use futures_channel::mpsc::UnboundedReceiver;
use futures_util::StreamExt;
use log::{info, warn};
use miette::{miette, IntoDiagnostic};
use miette::{Context, Result};
use shared::messages::heartbeat::WorkerHeartbeatResponse;
use shared::messages::queue::WorkerFrameQueueItemFinishedEvent;
use shared::messages::WebSocketMessage;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

/// Manages all incoming WebSocket messages for a worker.
pub struct WorkerEventDispatcher {
    /// `tokio`'s broadcast `Sender`. `subscribe()` to receive worker heartbeat responses.
    heartbeat_response_sender: Arc<broadcast::Sender<WorkerHeartbeatResponse>>,

    // TODO Need new events here.

    // `tokio`'s broadcast `Sender`. `subscribe()` to receive worker's queue item finished events.
    queue_item_finished_sender: Arc<broadcast::Sender<WorkerFrameQueueItemFinishedEvent>>,

    /// `JoinHandle` for the async task that is running this dispatcher.
    /// As long as this task is running, receivers from this dispatcher will receive incoming messages.
    #[allow(dead_code)]
    task_join_handle: JoinHandle<()>,
}

impl WorkerEventDispatcher {
    /// Initialize a new `WorkerEventDispatcher`, consuming the WebSocket connection's async receiver channel.
    pub async fn new(websocket_receiver_channel: UnboundedReceiver<WebSocketMessage>) -> Self {
        let (heartbeat_tx, _) = broadcast::channel::<WorkerHeartbeatResponse>(512);
        let (queue_item_finished_tx, _) =
            broadcast::channel::<WorkerFrameQueueItemFinishedEvent>(512);

        let heartbeat_tx_arc = Arc::new(heartbeat_tx);
        let queue_item_finished_tx_arc = Arc::new(queue_item_finished_tx);

        let dispatcher_join_handle = tokio::spawn(Self::run(
            heartbeat_tx_arc.clone(),
            queue_item_finished_tx_arc.clone(),
            websocket_receiver_channel,
        ));

        Self {
            heartbeat_response_sender: heartbeat_tx_arc,
            queue_item_finished_sender: queue_item_finished_tx_arc,
            task_join_handle: dispatcher_join_handle,
        }
    }

    /// Run the event dispatcher indefinitely, reading the incoming WebSocket messages
    /// and broadcasting them to subscribed `tokio::broadcast::Receiver`s.
    async fn run(
        heartbeat_channel_event_sender: Arc<broadcast::Sender<WorkerHeartbeatResponse>>,
        queue_item_finished_event_sender: Arc<broadcast::Sender<WorkerFrameQueueItemFinishedEvent>>,
        mut websocket_receiver_channel: UnboundedReceiver<WebSocketMessage>,
    ) {
        info!("WorkerEventDispatcher: Running event distribution loop.");

        loop {
            let next_message = match websocket_receiver_channel.next().await {
                Some(message) => message,
                None => {
                    warn!("WorkerEventDispatcher: Can't receive message from channel, aborting event dispatcher task.");
                    return;
                }
            };

            // We ignore the errors when `.send`-ing because the only reason `send` can fail
            // is when there are no receivers. We don't want to propagate such an error
            // (as it really isn't an error, it's just that no-one will see that message as no-one is subscribed).
            match next_message {
                WebSocketMessage::WorkerFrameQueueItemFinishedEvent(notification) => {
                    let _ = queue_item_finished_event_sender.send(notification);
                }
                WebSocketMessage::WorkerHeartbeatResponse(response) => {
                    let _ = heartbeat_channel_event_sender.send(response);
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

    /// Get a `Receiver` for future "queue item finished" worker messages.
    pub fn queue_item_finished_receiver(
        &self,
    ) -> broadcast::Receiver<WorkerFrameQueueItemFinishedEvent> {
        self.queue_item_finished_sender.subscribe()
    }

    /// Get a `Receiver` for future "heartbeat response" worker messages.
    pub fn heartbeat_response_receiver(&self) -> broadcast::Receiver<WorkerHeartbeatResponse> {
        self.heartbeat_response_sender.subscribe()
    }

    /*
     * One-shot async event methods
     */

    /// One-shot event method that completes when either some worker's queue item has been finished
    /// or when the `timeout` times out, whichever is sooner.
    ///
    /// If timed out, `Err` is returned.
    pub async fn wait_for_queue_item_finished(
        &self,
        timeout: Duration,
    ) -> Result<WorkerFrameQueueItemFinishedEvent> {
        let queue_item_finished_future = async {
            self.queue_item_finished_receiver()
                .recv()
                .await
                .into_diagnostic()
                .wrap_err_with(|| {
                    miette!("Could not receive queue item finished notification through channel.")
                })
        };

        let timeout_future = tokio::time::timeout(timeout, queue_item_finished_future).await;
        match timeout_future {
            Ok(notification_result) => notification_result,
            Err(_) => Err(miette!(
                "Waiting for queue item finished notification timed out."
            )),
        }
    }

    /// One-shot event method that completes when the worker sends the next heartbeat response
    /// or when the `timeout` times out, whichever is sooner.
    ///
    /// If timed out, `Err` is returned.
    pub async fn wait_for_heartbeat_response(
        &self,
        timeout: Duration,
    ) -> Result<WorkerHeartbeatResponse> {
        let heartbeat_response_future = async {
            self.heartbeat_response_receiver()
                .recv()
                .await
                .into_diagnostic()
                .wrap_err_with(|| miette!("Could not receive heartbeat response through channel."))
        };

        let timeout_future = tokio::time::timeout(timeout, heartbeat_response_future).await;
        match timeout_future {
            Ok(response_result) => match response_result {
                Ok(response) => Ok(response),
                Err(error) => Err(error),
            },
            Err(_) => Err(miette!(
                "Waiting for heartbeat response timed out."
            )),
        }
    }
}
