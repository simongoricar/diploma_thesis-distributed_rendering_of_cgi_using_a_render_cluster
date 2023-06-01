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

pub struct MasterEventDispatcher {
    heartbeat_request_sender: Arc<broadcast::Sender<MasterHeartbeatRequest>>,

    #[allow(dead_code)]
    heartbeat_request_receiver: broadcast::Receiver<MasterHeartbeatRequest>,

    queue_frame_add_request_sender: Arc<broadcast::Sender<MasterFrameQueueAddRequest>>,

    #[allow(dead_code)]
    queue_frame_add_request_receiver: broadcast::Receiver<MasterFrameQueueAddRequest>,

    queue_frame_remove_request_sender: Arc<broadcast::Sender<MasterFrameQueueRemoveRequest>>,

    #[allow(dead_code)]
    queue_frame_remove_request_receiver: broadcast::Receiver<MasterFrameQueueRemoveRequest>,
}

impl MasterEventDispatcher {
    pub async fn new(receiver_channel: UnboundedReceiver<WebSocketMessage>) -> Self {
        let (heartbeat_request_tx, heartbeat_request_rx) =
            broadcast::channel::<MasterHeartbeatRequest>(512);
        let (queue_frame_add_request_tx, queue_frame_add_request_rx) =
            broadcast::channel::<MasterFrameQueueAddRequest>(512);
        let (queue_frame_remove_request_tx, queue_frame_remove_request_rx) =
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
            heartbeat_request_receiver: heartbeat_request_rx,
            queue_frame_add_request_sender: queue_frame_add_request_tx_arc,
            queue_frame_add_request_receiver: queue_frame_add_request_rx,
            queue_frame_remove_request_sender: queue_frame_remove_request_tx_arc,
            queue_frame_remove_request_receiver: queue_frame_remove_request_rx,
        }
    }

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

            match next_message {
                WebSocketMessage::MasterHeartbeatRequest(request) => {
                    heartbeat_request_event_sender
                        .send(request)
                        .expect("Could not send heartbeat request event.");
                }
                WebSocketMessage::MasterFrameQueueAddRequest(request) => {
                    queue_frame_add_request_event_sender
                        .send(request)
                        .expect("Could not send frame queue add request event.");
                }
                WebSocketMessage::MasterFrameQueueRemoveRequest(request) => {
                    queue_frame_remove_request_event_sender
                        .send(request)
                        .expect("Could not send frame queue remove request event.");
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

    pub fn heartbeat_request_receiver(&self) -> broadcast::Receiver<MasterHeartbeatRequest> {
        self.heartbeat_request_sender.subscribe()
    }

    pub fn frame_queue_add_request_receiver(
        &self,
    ) -> broadcast::Receiver<MasterFrameQueueAddRequest> {
        self.queue_frame_add_request_sender.subscribe()
    }

    pub fn frame_queue_remove_request_receiver(
        &self,
    ) -> broadcast::Receiver<MasterFrameQueueRemoveRequest> {
        self.queue_frame_remove_request_sender.subscribe()
    }

    /*
     * One-shot async event methods
     */

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
