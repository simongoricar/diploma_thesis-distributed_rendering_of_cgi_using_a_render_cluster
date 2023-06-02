use std::sync::Arc;
use std::time::Duration;

use futures_channel::mpsc::UnboundedReceiver;
use futures_util::StreamExt;
use log::{info, trace, warn};
use miette::{miette, Context, IntoDiagnostic, Result};
use shared::cancellation::CancellationToken;
use shared::messages::heartbeat::MasterHeartbeatRequest;
use shared::messages::job::MasterJobFinishedEvent;
use shared::messages::queue::{MasterFrameQueueAddRequest, MasterFrameQueueRemoveRequest};
use shared::messages::WebSocketMessage;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

/// Manages all incoming WebSocket messages from the master server.
pub struct MasterEventDispatcher {
    /// Receives heartbeat requests.
    heartbeat_request_sender: Arc<broadcast::Sender<MasterHeartbeatRequest>>,

    /// Receives frame queue item adding requests.
    queue_frame_add_request_sender: Arc<broadcast::Sender<MasterFrameQueueAddRequest>>,

    /// Receives frame queue item removal requests.
    queue_frame_remove_request_sender: Arc<broadcast::Sender<MasterFrameQueueRemoveRequest>>,

    /// Receives job finished events.
    job_finished_event_sender: Arc<broadcast::Sender<MasterJobFinishedEvent>>,

    task_join_handle: JoinHandle<()>,
}

impl MasterEventDispatcher {
    /// Initialize a new `MasterEventDispatcher`, consuming the WebSocket connection's
    /// async receiver channel.
    pub async fn new(
        receiver_channel: UnboundedReceiver<WebSocketMessage>,
        cancellation_token: CancellationToken,
    ) -> Self {
        let (heartbeat_request_tx, _) = broadcast::channel::<MasterHeartbeatRequest>(512);
        let (queue_frame_add_request_tx, _) = broadcast::channel::<MasterFrameQueueAddRequest>(512);
        let (queue_frame_remove_request_tx, _) =
            broadcast::channel::<MasterFrameQueueRemoveRequest>(512);
        let (job_finished_event_tx, _) = broadcast::channel::<MasterJobFinishedEvent>(512);

        let heartbeat_request_tx_arc = Arc::new(heartbeat_request_tx);
        let queue_frame_add_request_tx_arc = Arc::new(queue_frame_add_request_tx);
        let queue_frame_remove_request_tx_arc = Arc::new(queue_frame_remove_request_tx);
        let job_finished_event_tx_arc = Arc::new(job_finished_event_tx);

        let task_join_handle = tokio::spawn(Self::run(
            heartbeat_request_tx_arc.clone(),
            queue_frame_add_request_tx_arc.clone(),
            queue_frame_remove_request_tx_arc.clone(),
            job_finished_event_tx_arc.clone(),
            receiver_channel,
            cancellation_token,
        ));

        Self {
            heartbeat_request_sender: heartbeat_request_tx_arc,
            queue_frame_add_request_sender: queue_frame_add_request_tx_arc,
            queue_frame_remove_request_sender: queue_frame_remove_request_tx_arc,
            job_finished_event_sender: job_finished_event_tx_arc,
            task_join_handle,
        }
    }

    pub async fn join(self) -> Result<()> {
        self.task_join_handle.await.into_diagnostic()
    }

    /// Run the event dispatcher indefinitely, reading the incoming WebSocket messages
    /// and broadcasting the to subscribed `tokio::broadcast::Receiver`s.
    async fn run(
        heartbeat_request_event_sender: Arc<broadcast::Sender<MasterHeartbeatRequest>>,
        queue_frame_add_request_event_sender: Arc<broadcast::Sender<MasterFrameQueueAddRequest>>,
        queue_frame_remove_request_event_sender: Arc<
            broadcast::Sender<MasterFrameQueueRemoveRequest>,
        >,
        job_finished_event_sender: Arc<broadcast::Sender<MasterJobFinishedEvent>>,
        mut websocket_receiver_channel: UnboundedReceiver<WebSocketMessage>,
        cancellation_token: CancellationToken,
    ) {
        info!("MasterEventDispatcher: Running event distribution loop.");

        loop {
            if cancellation_token.cancelled() {
                trace!("MasterEventDispatcher: stopping distribution (worker stopping).");
                break;
            }

            let next_message = if let Ok(message) = tokio::time::timeout(
                Duration::from_secs(2),
                websocket_receiver_channel.next(),
            )
            .await
            {
                match message {
                    Some(message) => message,
                    None => {
                        warn!("MasterEventDispatcher: Can't receiver message from channel, aborting event dispatcher loop.");
                        return;
                    }
                }
            } else {
                continue;
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
                WebSocketMessage::MasterJobFinishedEvent(event) => {
                    let _ = job_finished_event_sender.send(event);
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

    pub fn job_finished_event_receiver(&self) -> broadcast::Receiver<MasterJobFinishedEvent> {
        self.job_finished_event_sender.subscribe()
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
