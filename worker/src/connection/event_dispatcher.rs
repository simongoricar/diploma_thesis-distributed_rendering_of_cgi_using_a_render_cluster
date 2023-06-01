use std::sync::Arc;
use std::time::Duration;

use futures_channel::mpsc::UnboundedReceiver;
use futures_util::StreamExt;
use log::{info, warn};
use miette::{miette, Context, IntoDiagnostic, Result};
use shared::messages::heartbeat::MasterHeartbeatRequest;
use shared::messages::queue::{MasterFrameQueueAddRequest, MasterFrameQueueRemoveRequest};
use shared::messages::WebSocketMessage;
use tokio::sync::broadcast;

/// Manages all incoming WebSocket messages from the master server.
pub struct MasterEventDispatcher {
    /// `tokio`'s broadcast `Sender`. `subscribe()` to receive heartbeat requests.
    heartbeat_request_sender: Arc<broadcast::Sender<MasterHeartbeatRequest>>,

    /// `tokio`'s broadcast `Sender`. `subscribe()` to receive queue item add requests.
    queue_frame_add_request_sender: Arc<broadcast::Sender<MasterFrameQueueAddRequest>>,

    /// `tokio`'s broadcast `Sender`. `subscribe()` to receive queue item remove requests.
    queue_frame_remove_request_sender: Arc<broadcast::Sender<MasterFrameQueueRemoveRequest>>,
}

impl MasterEventDispatcher {
    /// Initialize a new `MasterEventDispatcher`, consuming the WebSocket connection's
    /// async receiver channel.
    pub async fn new(receiver_channel: UnboundedReceiver<WebSocketMessage>) -> Self {
        let (heartbeat_request_tx, _) = broadcast::channel::<MasterHeartbeatRequest>(512);
        let (queue_frame_add_request_tx, _) = broadcast::channel::<MasterFrameQueueAddRequest>(512);
        let (queue_frame_remove_request_tx, _) =
            broadcast::channel::<MasterFrameQueueRemoveRequest>(512);

        let heartbeat_request_tx_arc = Arc::new(heartbeat_request_tx);
        let queue_frame_add_request_tx_arc = Arc::new(queue_frame_add_request_tx);
        let queue_frame_remove_request_tx_arc = Arc::new(queue_frame_remove_request_tx);

        tokio::spawn(Self::run(
            heartbeat_request_tx_arc.clone(),
            queue_frame_add_request_tx_arc.clone(),
            queue_frame_remove_request_tx_arc.clone(),
            receiver_channel,
        ));

        Self {
            heartbeat_request_sender: heartbeat_request_tx_arc,
            queue_frame_add_request_sender: queue_frame_add_request_tx_arc,
            queue_frame_remove_request_sender: queue_frame_remove_request_tx_arc,
        }
    }

    /// Run the event dispatcher indefinitely, reading the incoming WebSocket messages
    /// and broadcasting the to subscribed `tokio::broadcast::Receiver`s.
    async fn run(
        heartbeat_request_event_sender: Arc<broadcast::Sender<MasterHeartbeatRequest>>,
        queue_frame_add_request_event_sender: Arc<broadcast::Sender<MasterFrameQueueAddRequest>>,
        queue_frame_remove_request_event_sender: Arc<
            broadcast::Sender<MasterFrameQueueRemoveRequest>,
        >,
        mut websocket_receiver_channel: UnboundedReceiver<WebSocketMessage>,
    ) {
        info!("MasterEventDispatcher: Running event distribution loop.");

        loop {
            let next_message = match websocket_receiver_channel.next().await {
                Some(message) => message,
                None => {
                    warn!("MasterEventDispatcher: Can't receiver message from channel, aborting event dispatcher loop.");
                    return;
                }
            };

            // We ignore the errors when `.send`-ing because the only reason `send` can fail
            // is when there are no receivers. We don't want to propagate such an error
            // (as it really isn't an error, it's just that no-one will see that message as no-one is subscribed).
            match next_message {
                WebSocketMessage::MasterHeartbeatRequest(request) => {
                    let _ = heartbeat_request_event_sender.send(request);
                }
                WebSocketMessage::MasterFrameQueueAddRequest(request) => {
                    let _ = queue_frame_add_request_event_sender.send(request);
                }
                WebSocketMessage::MasterFrameQueueRemoveRequest(request) => {
                    let _ = queue_frame_remove_request_event_sender.send(request);
                }
                _ => {
                    warn!("MasterEventDispatcher: Unexpected (but valid) incoming WebSocket message: {}", next_message.type_name());
                }
            }
        }
    }

    /*
     * Public event channel methods
     */

    /// Get a `Receiver` for future heartbeat requests from the master server.
    pub fn heartbeat_request_receiver(&self) -> broadcast::Receiver<MasterHeartbeatRequest> {
        self.heartbeat_request_sender.subscribe()
    }

    /// Get a `Receiver` for future "queue item add" requests from the master server.
    pub fn frame_queue_add_request_receiver(
        &self,
    ) -> broadcast::Receiver<MasterFrameQueueAddRequest> {
        self.queue_frame_add_request_sender.subscribe()
    }

    /// Get a `Receiver` for future "queue item remove" requests from the master server.
    pub fn frame_queue_remove_request_receiver(
        &self,
    ) -> broadcast::Receiver<MasterFrameQueueRemoveRequest> {
        self.queue_frame_remove_request_sender.subscribe()
    }

    /*
     * One-shot async event methods
     */

    /// One-shot event method that completes when we either receive a heartbeat request
    /// or we time out (see `timeout`), whichever is sooner.
    ///
    /// If timed out, `Err` is returned.
    #[allow(dead_code)]
    pub async fn wait_for_heartbeat_request(
        &self,
        timeout: Duration,
    ) -> Result<MasterHeartbeatRequest> {
        let heartbeat_request_future = async {
            self.heartbeat_request_receiver()
                .recv()
                .await
                .into_diagnostic()
                .wrap_err_with(|| miette!("Could not receive heartbeat finished notification."))
        };

        let timeout_future = tokio::time::timeout(timeout, heartbeat_request_future).await;
        match timeout_future {
            Ok(heartbeat_request_result) => heartbeat_request_result,
            Err(_) => Err(miette!(
                "Waiting for heartbeat request timed out."
            )),
        }
    }

    /// One-shot event method that completes when we either receive a queue item adding request
    /// or we time out (see `timeout`), whichever is sooner.
    ///
    /// If timed out, `Err` is returned.
    #[allow(dead_code)]
    pub async fn wait_for_queue_add_request(
        &self,
        timeout: Duration,
    ) -> Result<MasterFrameQueueAddRequest> {
        let frame_queue_add_request_future = async {
            self.frame_queue_add_request_receiver()
                .recv()
                .await
                .into_diagnostic()
                .wrap_err_with(|| miette!("Could not receive queue add request."))
        };

        let timeout_future = tokio::time::timeout(timeout, frame_queue_add_request_future).await;
        match timeout_future {
            Ok(frame_queue_add_request_result) => frame_queue_add_request_result,
            Err(_) => Err(miette!(
                "Waiting for frame queue add request timed out."
            )),
        }
    }

    /// One-shot event method that completes when we either receive a queue item removal request
    /// or we time out (see `timeout`), whichever is sooner.
    ///
    /// If timed out, `Err` is returned.
    #[allow(dead_code)]
    pub async fn wait_for_queue_remove_request(
        &self,
        timeout: Duration,
    ) -> Result<MasterFrameQueueRemoveRequest> {
        let frame_queue_remove_request_future = async {
            self.frame_queue_remove_request_receiver()
                .recv()
                .await
                .into_diagnostic()
                .wrap_err_with(|| miette!("Could not receive queue remove request."))
        };

        let timeout_future = tokio::time::timeout(timeout, frame_queue_remove_request_future).await;
        match timeout_future {
            Ok(frame_queue_remove_request_result) => frame_queue_remove_request_result,
            Err(_) => Err(miette!(
                "Waiting for frame queue add request timed out."
            )),
        }
    }
}
