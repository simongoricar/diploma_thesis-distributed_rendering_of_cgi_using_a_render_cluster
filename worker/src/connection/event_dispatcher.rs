use std::sync::Arc;
use std::time::Duration;

use futures_channel::mpsc::UnboundedReceiver;
use futures_util::StreamExt;
use log::{debug, info, trace, warn};
use miette::{miette, Context, IntoDiagnostic, Result};
use shared::cancellation::CancellationToken;
use shared::messages::heartbeat::MasterHeartbeatRequest;
use shared::messages::job::{MasterJobFinishedRequest, MasterJobStartedEvent};
use shared::messages::queue::{MasterFrameQueueAddRequest, MasterFrameQueueRemoveRequest};
use shared::messages::WebSocketMessage;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

#[derive(Clone)]
pub struct EventSenders {
    /// Receives heartbeat requests.
    heartbeat_request: Arc<broadcast::Sender<MasterHeartbeatRequest>>,

    /// Receives frame queue item adding requests.
    queue_frame_add_request: Arc<broadcast::Sender<MasterFrameQueueAddRequest>>,

    /// Receives frame queue item removal requests.
    queue_frame_remove_request: Arc<broadcast::Sender<MasterFrameQueueRemoveRequest>>,

    job_started_event: Arc<broadcast::Sender<MasterJobStartedEvent>>,

    /// Receives job finishing requests.
    job_finished_request: Arc<broadcast::Sender<MasterJobFinishedRequest>>,
}


/// Manages all incoming WebSocket messages from the master server.
pub struct MasterEventDispatcher {
    senders: EventSenders,

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
        let (job_started_event_tx, _) = broadcast::channel::<MasterJobStartedEvent>(512);
        let (job_finished_request_tx, _) = broadcast::channel::<MasterJobFinishedRequest>(512);

        let heartbeat_request_tx_arc = Arc::new(heartbeat_request_tx);
        let queue_frame_add_request_tx_arc = Arc::new(queue_frame_add_request_tx);
        let queue_frame_remove_request_tx_arc = Arc::new(queue_frame_remove_request_tx);
        let job_started_event_tx_arc = Arc::new(job_started_event_tx);
        let job_finished_request_tx_arc = Arc::new(job_finished_request_tx);

        let senders = EventSenders {
            heartbeat_request: heartbeat_request_tx_arc,
            queue_frame_add_request: queue_frame_add_request_tx_arc,
            queue_frame_remove_request: queue_frame_remove_request_tx_arc,
            job_started_event: job_started_event_tx_arc,
            job_finished_request: job_finished_request_tx_arc,
        };

        let task_join_handle = tokio::spawn(Self::run(
            senders.clone(),
            receiver_channel,
            cancellation_token,
        ));

        Self {
            senders,
            task_join_handle,
        }
    }

    pub async fn join(self) -> Result<()> {
        self.task_join_handle.await.into_diagnostic()
    }

    /// Run the event dispatcher indefinitely, reading the incoming WebSocket messages
    /// and broadcasting the to subscribed `tokio::broadcast::Receiver`s.
    async fn run(
        senders: EventSenders,
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
                        warn!("MasterEventDispatcher: Can't receive message from channel, aborting event dispatcher loop.");
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
                    debug!("Received message: master heartbeat request.");
                    let _ = senders.heartbeat_request.send(request);
                }
                WebSocketMessage::MasterFrameQueueAddRequest(request) => {
                    debug!("Received message: master frame queue add request.");
                    let _ = senders.queue_frame_add_request.send(request);
                }
                WebSocketMessage::MasterFrameQueueRemoveRequest(request) => {
                    debug!("Received message: master frame queue remove request.");
                    let _ = senders.queue_frame_remove_request.send(request);
                }
                WebSocketMessage::MasterJobStartedEvent(event) => {
                    debug!("Received message: job started event.");
                    let _ = senders.job_started_event.send(event);
                }
                WebSocketMessage::MasterJobFinishedRequest(request) => {
                    debug!("Received message: job finished request.");
                    let _ = senders.job_finished_request.send(request);
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
        self.senders.heartbeat_request.subscribe()
    }

    /// Get a `Receiver` for future "queue item add" requests from the master server.
    pub fn frame_queue_add_request_receiver(
        &self,
    ) -> broadcast::Receiver<MasterFrameQueueAddRequest> {
        self.senders.queue_frame_add_request.subscribe()
    }

    /// Get a `Receiver` for future "queue item remove" requests from the master server.
    pub fn frame_queue_remove_request_receiver(
        &self,
    ) -> broadcast::Receiver<MasterFrameQueueRemoveRequest> {
        self.senders.queue_frame_remove_request.subscribe()
    }

    pub fn job_started_event_receiver(&self) -> broadcast::Receiver<MasterJobStartedEvent> {
        self.senders.job_started_event.subscribe()
    }

    pub fn job_finished_request_receiver(&self) -> broadcast::Receiver<MasterJobFinishedRequest> {
        self.senders.job_finished_request.subscribe()
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
