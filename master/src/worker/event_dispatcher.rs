use std::sync::Arc;
use std::time::Duration;

use futures_channel::mpsc::UnboundedReceiver;
use futures_util::StreamExt;
use log::{info, warn};
use miette::{miette, IntoDiagnostic};
use miette::{Context, Result};
use shared::messages::heartbeat::WorkerHeartbeatResponse;
use shared::messages::queue::WorkerFrameQueueItemFinishedNotification;
use shared::messages::WebSocketMessage;
use tokio::sync::broadcast;

pub struct WorkerEventDispatcher {
    heartbeat_response_sender: Arc<broadcast::Sender<WorkerHeartbeatResponse>>,

    #[allow(dead_code)]
    heartbeat_response_receiver: broadcast::Receiver<WorkerHeartbeatResponse>,

    queue_item_finished_sender: Arc<broadcast::Sender<WorkerFrameQueueItemFinishedNotification>>,

    #[allow(dead_code)]
    queue_item_finished_receiver: broadcast::Receiver<WorkerFrameQueueItemFinishedNotification>,
}

impl WorkerEventDispatcher {
    pub async fn new(websocket_receiver_channel: UnboundedReceiver<WebSocketMessage>) -> Self {
        let (heartbeat_tx, heartbeat_rx) = broadcast::channel::<WorkerHeartbeatResponse>(512);
        let (queue_item_finished_tx, queue_item_finished_rx) =
            broadcast::channel::<WorkerFrameQueueItemFinishedNotification>(512);

        let heartbeat_tx_arc = Arc::new(heartbeat_tx);
        let queue_item_finished_tx_arc = Arc::new(queue_item_finished_tx);

        tokio::spawn(Self::run(
            heartbeat_tx_arc.clone(),
            queue_item_finished_tx_arc.clone(),
            websocket_receiver_channel,
        ));

        Self {
            heartbeat_response_sender: heartbeat_tx_arc,
            heartbeat_response_receiver: heartbeat_rx,
            queue_item_finished_sender: queue_item_finished_tx_arc,
            queue_item_finished_receiver: queue_item_finished_rx,
        }
    }

    async fn run(
        heartbeat_channel_event_sender: Arc<broadcast::Sender<WorkerHeartbeatResponse>>,
        queue_item_finished_event_sender: Arc<
            broadcast::Sender<WorkerFrameQueueItemFinishedNotification>,
        >,
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

            match next_message {
                WebSocketMessage::WorkerFrameQueueItemFinishedNotification(notification) => {
                    queue_item_finished_event_sender
                        .send(notification)
                        .expect("Could not send queue item finished event.");
                }
                WebSocketMessage::WorkerHeartbeatResponse(response) => {
                    heartbeat_channel_event_sender
                        .send(response)
                        .expect("Could not send heartbeat response event.");
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

    pub fn queue_item_finished_receiver(
        &self,
    ) -> broadcast::Receiver<WorkerFrameQueueItemFinishedNotification> {
        self.queue_item_finished_sender.subscribe()
    }

    pub fn heartbeat_response_receiver(&self) -> broadcast::Receiver<WorkerHeartbeatResponse> {
        self.heartbeat_response_sender.subscribe()
    }

    /*
     * One-shot async event methods
     */

    pub async fn wait_for_queue_item_finished(
        &self,
        timeout: Duration,
    ) -> Result<WorkerFrameQueueItemFinishedNotification> {
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
