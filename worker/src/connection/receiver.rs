use std::sync::Arc;
use std::time::Duration;

use futures_util::stream::SplitStream;
use futures_util::StreamExt;
use log::{debug, info, trace, warn};
use miette::{miette, Context, IntoDiagnostic, Result};
use shared::cancellation::CancellationToken;
use shared::messages::handshake::{MasterHandshakeAcknowledgement, MasterHandshakeRequest};
use shared::messages::heartbeat::MasterHeartbeatRequest;
use shared::messages::job::{MasterJobFinishedRequest, MasterJobStartedEvent};
use shared::messages::queue::{MasterFrameQueueAddRequest, MasterFrameQueueRemoveRequest};
use shared::messages::traits::Message;
use shared::messages::{parse_websocket_message, WebSocketMessage};
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio_tungstenite::WebSocketStream;

#[derive(Clone, Debug)]
pub struct BroadcastSenders {
    handshake_request: Arc<Sender<MasterHandshakeRequest>>,

    handshake_ack: Arc<Sender<MasterHandshakeAcknowledgement>>,

    /// Receives frame queue item adding requests.
    queue_frame_add_request: Arc<Sender<MasterFrameQueueAddRequest>>,

    /// Receives frame queue item removal requests.
    queue_frame_remove_request: Arc<Sender<MasterFrameQueueRemoveRequest>>,

    /// Receives heartbeat requests.
    heartbeat_request: Arc<Sender<MasterHeartbeatRequest>>,

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
        websocket_stream: SplitStream<WebSocketStream<TcpStream>>,
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
        let (heartbeat_request_tx, _) =
            broadcast::channel::<MasterHeartbeatRequest>(RECEIVER_BROADCAST_CHANNEL_SIZE);
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
            websocket_stream,
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
        mut websocket_stream: SplitStream<WebSocketStream<TcpStream>>,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        info!("MasterReceiver: Running event distribution loop.");

        loop {
            let next_message_result =
                tokio::time::timeout(Duration::from_secs(2), websocket_stream.next()).await;

            if cancellation_token.is_cancelled() {
                trace!("MasterReceiver: stopping distribution (worker stopping).");
                break;
            }

            let raw_message = match next_message_result {
                Ok(potential_message) => potential_message
                    .ok_or_else(|| miette!("Could not read from WebSocketStream - None."))?
                    .into_diagnostic()
                    .wrap_err_with(|| miette!("Errored while receiving from WebSocketStream"))?,
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
                    let _ = senders.heartbeat_request.send(request);
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

    pub fn handshake_request_receiver(&self) -> Receiver<MasterHandshakeRequest> {
        self.senders.handshake_request.subscribe()
    }

    pub fn handshake_ack_receiver(&self) -> Receiver<MasterHandshakeAcknowledgement> {
        self.senders.handshake_ack.subscribe()
    }

    /// Get a `Receiver` for future "queue item add" requests from the master server.
    pub fn frame_queue_add_request_receiver(&self) -> Receiver<MasterFrameQueueAddRequest> {
        self.senders.queue_frame_add_request.subscribe()
    }

    /// Get a `Receiver` for future "queue item remove" requests from the master server.
    pub fn frame_queue_remove_request_receiver(&self) -> Receiver<MasterFrameQueueRemoveRequest> {
        self.senders.queue_frame_remove_request.subscribe()
    }

    /// Get a `Receiver` for future heartbeat requests from the master server.
    pub fn heartbeat_request_receiver(&self) -> Receiver<MasterHeartbeatRequest> {
        self.senders.heartbeat_request.subscribe()
    }

    pub fn job_started_event_receiver(&self) -> Receiver<MasterJobStartedEvent> {
        self.senders.job_started_event.subscribe()
    }

    pub fn job_finished_request_receiver(&self) -> Receiver<MasterJobFinishedRequest> {
        self.senders.job_finished_request.subscribe()
    }

    /*
     * One-shot async event methods
     */

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

    pub async fn wait_for_handshake_request(
        &self,
        receiver: Option<Receiver<MasterHandshakeRequest>>,
        timeout: Option<Duration>,
    ) -> Result<MasterHandshakeRequest> {
        self.wait_for_message(
            receiver.unwrap_or_else(|| self.handshake_request_receiver()),
            timeout,
        )
        .await
    }

    pub async fn wait_for_handshake_ack(
        &self,
        receiver: Option<Receiver<MasterHandshakeAcknowledgement>>,
        timeout: Option<Duration>,
    ) -> Result<MasterHandshakeAcknowledgement> {
        self.wait_for_message(
            receiver.unwrap_or_else(|| self.handshake_ack_receiver()),
            timeout,
        )
        .await
    }

    /// One-shot event method that completes when we either receive a heartbeat request
    /// or we time out (see `timeout`), whichever is sooner.
    ///
    /// If timed out, `Err` is returned.
    #[allow(dead_code)]
    pub async fn wait_for_heartbeat_request(
        &self,
        timeout: Option<Duration>,
    ) -> Result<MasterHeartbeatRequest> {
        self.wait_for_message(self.heartbeat_request_receiver(), timeout)
            .await
    }
}
