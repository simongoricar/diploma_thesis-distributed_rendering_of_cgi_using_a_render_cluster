use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use miette::{miette, Context, IntoDiagnostic, Result};
use shared::cancellation::CancellationToken;
use shared::messages::handshake::{MasterHandshakeAcknowledgement, MasterHandshakeRequest};
use shared::messages::heartbeat::MasterHeartbeatRequest;
use shared::messages::job::{MasterJobFinishedRequest, MasterJobStartedEvent};
use shared::messages::queue::{MasterFrameQueueAddRequest, MasterFrameQueueRemoveRequest};
use shared::messages::{parse_websocket_message, WebSocketMessage};
use tokio::sync::broadcast;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::task::JoinHandle;
use tracing::{debug, info, trace, warn};

use crate::connection::ReconnectingWebSocketClient;


#[derive(Clone, Debug)]
pub struct BroadcastSenders {
    handshake_request: Arc<Sender<MasterHandshakeRequest>>,

    handshake_ack: Arc<Sender<MasterHandshakeAcknowledgement>>,

    /// Receives frame queue item adding requests.
    queue_frame_add_request: Arc<Sender<MasterFrameQueueAddRequest>>,

    /// Receives frame queue item removal requests.
    queue_frame_remove_request: Arc<Sender<MasterFrameQueueRemoveRequest>>,

    /// Receives heartbeat requests.
    heartbeat_request: Arc<Sender<(MasterHeartbeatRequest, DateTime<Utc>)>>,

    job_started_event: Arc<Sender<MasterJobStartedEvent>>,

    /// Receives job finishing requests.
    job_finished_request: Arc<Sender<MasterJobFinishedRequest>>,
}

static RECEIVER_BROADCAST_CHANNEL_SIZE: usize = 512;


/// Manages all incoming WebSocket messages from the master server.
#[derive(Debug)]
pub struct MasterReceiver {
    senders: BroadcastSenders,

    task_join_handle: JoinHandle<Result<()>>,
}

impl MasterReceiver {
    /// Initialize a new `MasterEventDispatcher`, consuming the WebSocket connection's
    /// async receiver channel.
    pub fn new(
        client: Arc<ReconnectingWebSocketClient>,
        cancellation_token: CancellationToken,
    ) -> Self {
        // Initialize broadcast channels.
        let (handshake_request_tx, _) =
            broadcast::channel::<MasterHandshakeRequest>(RECEIVER_BROADCAST_CHANNEL_SIZE);
        let (handshake_ack_tx, _) =
            broadcast::channel::<MasterHandshakeAcknowledgement>(RECEIVER_BROADCAST_CHANNEL_SIZE);
        let (queue_frame_add_request_tx, _) =
            broadcast::channel::<MasterFrameQueueAddRequest>(RECEIVER_BROADCAST_CHANNEL_SIZE);
        let (queue_frame_remove_request_tx, _) =
            broadcast::channel::<MasterFrameQueueRemoveRequest>(RECEIVER_BROADCAST_CHANNEL_SIZE);
        let (heartbeat_request_tx, _) = broadcast::channel::<(MasterHeartbeatRequest, DateTime<Utc>)>(
            RECEIVER_BROADCAST_CHANNEL_SIZE,
        );
        let (job_started_event_tx, _) =
            broadcast::channel::<MasterJobStartedEvent>(RECEIVER_BROADCAST_CHANNEL_SIZE);
        let (job_finished_request_tx, _) =
            broadcast::channel::<MasterJobFinishedRequest>(RECEIVER_BROADCAST_CHANNEL_SIZE);

        // Wrap broadcast channels in `Arc`s and construct a `BroadcastSenders` to house them.
        let handshake_request_tx_arc = Arc::new(handshake_request_tx);
        let handshake_ack_tx_arc = Arc::new(handshake_ack_tx);
        let heartbeat_request_tx_arc = Arc::new(heartbeat_request_tx);
        let queue_frame_add_request_tx_arc = Arc::new(queue_frame_add_request_tx);
        let queue_frame_remove_request_tx_arc = Arc::new(queue_frame_remove_request_tx);
        let job_started_event_tx_arc = Arc::new(job_started_event_tx);
        let job_finished_request_tx_arc = Arc::new(job_finished_request_tx);

        let senders = BroadcastSenders {
            handshake_request: handshake_request_tx_arc,
            handshake_ack: handshake_ack_tx_arc,
            queue_frame_add_request: queue_frame_add_request_tx_arc,
            queue_frame_remove_request: queue_frame_remove_request_tx_arc,
            heartbeat_request: heartbeat_request_tx_arc,
            job_started_event: job_started_event_tx_arc,
            job_finished_request: job_finished_request_tx_arc,
        };

        let task_join_handle = tokio::spawn(Self::run(
            senders.clone(),
            client,
            cancellation_token,
        ));


        Self {
            senders,
            task_join_handle,
        }
    }

    pub async fn join(self) -> Result<()> {
        self.task_join_handle.await.into_diagnostic()?
    }

    /// Run the event dispatcher indefinitely, reading the incoming WebSocket messages
    /// and broadcasting the to subscribed `tokio::broadcast::Receiver`s.
    async fn run(
        senders: BroadcastSenders,
        client: Arc<ReconnectingWebSocketClient>,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        info!("MasterReceiver: Running event distribution loop.");

        loop {
            let next_message_result =
                tokio::time::timeout(Duration::from_secs(2), client.receive_message()).await;

            if cancellation_token.is_cancelled() {
                trace!("MasterReceiver: stopping distribution (worker stopping).");
                break;
            }

            let raw_message = match next_message_result {
                Ok(potential_message) => potential_message.wrap_err_with(|| {
                    miette!("Errored while receiving from reconnecting client.")
                })?,
                // Simply wait two more seconds for the next message
                // (this Err happens when we time out on waiting for the next message).
                Err(_) => continue,
            };

            let parsed_message = {
                let optional_message = parse_websocket_message(raw_message)
                    .wrap_err_with(|| miette!("Could not parse WebSocket message."))?;

                if let Some(message) = optional_message {
                    message
                } else {
                    // Simply not a WS message we should care about (Ping/Pong/Frame/etc.).
                    continue;
                }
            };

            // We ignore the errors when `.send`-ing because the only reason `send` can fail
            // is when there are no receivers. We don't want to propagate such an error
            // (as it really isn't an error, it's just that no-one will see that message as no-one is subscribed).
            match parsed_message {
                WebSocketMessage::MasterHandshakeRequest(request) => {
                    debug!("Received message: master handshake request.");
                    let _ = senders.handshake_request.send(request);
                }
                WebSocketMessage::MasterHandshakeAcknowledgement(ack) => {
                    debug!("Received message: master handshake ack.");
                    let _ = senders.handshake_ack.send(ack);
                }
                WebSocketMessage::MasterFrameQueueAddRequest(request) => {
                    debug!("Received message: master frame queue add request.");
                    let _ = senders.queue_frame_add_request.send(request);
                }
                WebSocketMessage::MasterFrameQueueRemoveRequest(request) => {
                    debug!("Received message: master frame queue remove request.");
                    let _ = senders.queue_frame_remove_request.send(request);
                }
                WebSocketMessage::MasterHeartbeatRequest(request) => {
                    debug!("Received message: master heartbeat request.");
                    let time_now = Utc::now();
                    let _ = senders.heartbeat_request.send((request, time_now));
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
                    warn!(
                        "MasterReceiver: Unexpected (but valid) incoming WebSocket message: {}",
                        parsed_message.type_name()
                    );
                }
            }
        }

        Ok(())
    }

    /*
     * Public event channel methods
     */

    /// Get a `Receiver` for future "queue item add" requests from the master server.
    pub fn frame_queue_add_request_receiver(&self) -> Receiver<MasterFrameQueueAddRequest> {
        self.senders.queue_frame_add_request.subscribe()
    }

    /// Get a `Receiver` for future "queue item remove" requests from the master server.
    pub fn frame_queue_remove_request_receiver(&self) -> Receiver<MasterFrameQueueRemoveRequest> {
        self.senders.queue_frame_remove_request.subscribe()
    }

    /// Get a `Receiver` for future heartbeat requests from the master server.
    pub fn heartbeat_request_receiver(&self) -> Receiver<(MasterHeartbeatRequest, DateTime<Utc>)> {
        self.senders.heartbeat_request.subscribe()
    }

    /// Get a `Receiver` for future job start events from the master server.
    pub fn job_started_event_receiver(&self) -> Receiver<MasterJobStartedEvent> {
        self.senders.job_started_event.subscribe()
    }

    /// Get a `Receiver` for future job finished requests from the master server.
    pub fn job_finished_request_receiver(&self) -> Receiver<MasterJobFinishedRequest> {
        self.senders.job_finished_request.subscribe()
    }
}
