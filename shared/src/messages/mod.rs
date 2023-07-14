use std::sync::Arc;

use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures_channel::oneshot;
use futures_util::stream::StreamExt;
use miette::{miette, Context, IntoDiagnostic, Result};
use serde::{Deserialize, Serialize};
use tokio_tungstenite::tungstenite;
use tracing::debug;

use crate::messages::handshake::{
    MasterHandshakeAcknowledgement,
    MasterHandshakeRequest,
    WorkerHandshakeResponse,
};
use crate::messages::heartbeat::{MasterHeartbeatRequest, WorkerHeartbeatResponse};
use crate::messages::job::{
    MasterJobFinishedRequest,
    MasterJobStartedEvent,
    WorkerJobFinishedResponse,
};
use crate::messages::queue::{
    MasterFrameQueueAddRequest,
    MasterFrameQueueRemoveRequest,
    WorkerFrameQueueAddResponse,
    WorkerFrameQueueItemFinishedEvent,
    WorkerFrameQueueItemRenderingEvent,
    WorkerFrameQueueRemoveResponse,
};
use crate::messages::traits::Message;
use crate::messages::utilities::OutgoingMessageID;

pub mod handshake;
pub mod heartbeat;
pub mod job;
pub mod queue;
pub mod traits;
mod utilities;


#[derive(Clone)]
pub struct SenderHandle {
    sender_channel: Arc<UnboundedSender<OutgoingMessage>>,
}

impl SenderHandle {
    pub fn from_channel(sender: Arc<UnboundedSender<OutgoingMessage>>) -> Self {
        Self {
            sender_channel: sender,
        }
    }

    pub async fn send_message<M: Into<WebSocketMessage>>(&self, message: M) -> Result<()> {
        let (message, sent_receiver) =
            OutgoingMessage::from_message(message.into().to_tungstenite_message()?);
        let message_id = message.id;

        self.sender_channel
            .unbounded_send(message)
            .into_diagnostic()
            .wrap_err_with(|| miette!("Could not queue WebSocket message for sending."))?;

        debug!(
            message_id = %message_id,
            "Queued message for sending."
        );

        sent_receiver
            .await
            .into_diagnostic()
            .wrap_err_with(|| miette!("Errored while waiting for WebSocket message to be sent."))?;

        Ok(())
    }
}


pub struct OutgoingMessage {
    pub id: OutgoingMessageID,

    pub message: tungstenite::Message,

    pub oneshot_sender: oneshot::Sender<()>,
}

impl OutgoingMessage {
    pub fn from_message(outgoing_message: tungstenite::Message) -> (Self, oneshot::Receiver<()>) {
        let (oneshot_channel_tx, oneshot_channel_rx) = oneshot::channel::<()>();

        let new_self = Self {
            id: OutgoingMessageID::generate(),
            message: outgoing_message,
            oneshot_sender: oneshot_channel_tx,
        };

        (new_self, oneshot_channel_rx)
    }
}



pub fn parse_websocket_message(message: tungstenite::Message) -> Result<Option<WebSocketMessage>> {
    match message {
        tungstenite::Message::Text(text_message) => Ok(Some(WebSocketMessage::from_json_string(
            text_message,
        )?)),
        _ => Ok(None),
    }
}

pub async fn receive_exact_message_from_receiver<M: Message + TryFrom<WebSocketMessage>>(
    receiver_channel: &mut UnboundedReceiver<WebSocketMessage>,
) -> Result<M> {
    let next_message = receiver_channel
        .next()
        .await
        .ok_or_else(|| miette!("Could not get next incoming message."))?;

    if let Ok(message) = next_message.try_into() {
        Ok(message)
    } else {
        Err(miette!("Unexpected incoming message type."))
    }
}

pub async fn receive_exact_message_from_stream<
    M: Message + TryFrom<WebSocketMessage>,
    S: StreamExt<Item = core::result::Result<tungstenite::Message, tungstenite::error::Error>> + Unpin,
>(
    stream: &mut S,
) -> Result<M> {
    let ws_message = stream
        .next()
        .await
        .ok_or_else(|| miette!("Stream is empty, can't read."))?
        .into_diagnostic()
        .wrap_err_with(|| miette!("Failed to receive message from WebSocket stream."))?;

    let raw_message = WebSocketMessage::from_websocket_message(ws_message)
        .wrap_err_with(|| miette!("Could not decode received WS Message as WebSocketMessage."))?;

    if let Ok(expected_message) = M::try_from(raw_message) {
        Ok(expected_message)
    } else {
        Err(miette!("Unexpected message type."))
    }
}


#[derive(Serialize, Deserialize)]
#[serde(tag = "message_type", content = "payload")]
pub enum WebSocketMessage {
    /*
     * Handshake
     */
    #[serde(rename = "handshake_request")]
    MasterHandshakeRequest(MasterHandshakeRequest),

    #[serde(rename = "handshake_response")]
    WorkerHandshakeResponse(WorkerHandshakeResponse),

    #[serde(rename = "handshake_acknowledgement")]
    MasterHandshakeAcknowledgement(MasterHandshakeAcknowledgement),

    /*
     * Frame queue requests / responses
     */
    #[serde(rename = "request_frame-queue_add")]
    MasterFrameQueueAddRequest(MasterFrameQueueAddRequest),

    #[serde(rename = "response_frame-queue-add")]
    WorkerFrameQueueAddResponse(WorkerFrameQueueAddResponse),

    #[serde(rename = "request_frame-queue_remove")]
    MasterFrameQueueRemoveRequest(MasterFrameQueueRemoveRequest),

    #[serde(rename = "response_frame-queue_remove")]
    WorkerFrameQueueRemoveResponse(WorkerFrameQueueRemoveResponse),

    /*
     * Frame queue events
     */
    #[serde(rename = "event_frame-queue_item-started-rendering")]
    WorkerFrameQueueItemRenderingEvent(WorkerFrameQueueItemRenderingEvent),

    #[serde(rename = "event_frame-queue_item-finished")]
    WorkerFrameQueueItemFinishedEvent(WorkerFrameQueueItemFinishedEvent),

    /*
     * Heartbeats
     */
    #[serde(rename = "request_heartbeat")]
    MasterHeartbeatRequest(MasterHeartbeatRequest),

    #[serde(rename = "response_heartbeat")]
    WorkerHeartbeatResponse(WorkerHeartbeatResponse),

    /*
     * Job-related events and requests
     */
    #[serde(rename = "event_job-started")]
    MasterJobStartedEvent(MasterJobStartedEvent),

    #[serde(rename = "request_job-finished")]
    MasterJobFinishedRequest(MasterJobFinishedRequest),

    #[serde(rename = "response_job-finished")]
    WorkerJobFinishedResponse(WorkerJobFinishedResponse),
}

impl WebSocketMessage {
    pub fn from_json_string(string: String) -> Result<Self> {
        serde_json::from_str::<WebSocketMessage>(&string).into_diagnostic()
    }

    pub fn from_websocket_message(message: tungstenite::Message) -> Result<Self> {
        match message {
            tungstenite::Message::Text(string) => {
                let ws_message: WebSocketMessage =
                    serde_json::from_str(&string).into_diagnostic()?;
                Ok(ws_message)
            }
            tungstenite::Message::Binary(_) => {
                todo!("Not implemented, we don't handle binary websocket messages.");
            }
            _ => Err(miette!("Invalid WebSocket message type.")),
        }
    }

    pub fn to_tungstenite_message(&self) -> Result<tungstenite::Message> {
        let serialized_string = serde_json::to_string(self)
            .into_diagnostic()
            .wrap_err_with(|| miette!("Could not serialize to JSON."))?;

        Ok(tungstenite::Message::Text(serialized_string))
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            WebSocketMessage::MasterHandshakeRequest(_) => MasterHandshakeRequest::type_name(),
            WebSocketMessage::WorkerHandshakeResponse(_) => WorkerHandshakeResponse::type_name(),
            WebSocketMessage::MasterHandshakeAcknowledgement(_) => {
                MasterHandshakeAcknowledgement::type_name()
            }
            WebSocketMessage::MasterFrameQueueAddRequest(_) => {
                MasterFrameQueueAddRequest::type_name()
            }
            WebSocketMessage::WorkerFrameQueueAddResponse(_) => {
                WorkerFrameQueueAddResponse::type_name()
            }
            WebSocketMessage::MasterFrameQueueRemoveRequest(_) => {
                MasterFrameQueueRemoveRequest::type_name()
            }
            WebSocketMessage::WorkerFrameQueueRemoveResponse(_) => {
                WorkerFrameQueueRemoveResponse::type_name()
            }
            WebSocketMessage::WorkerFrameQueueItemRenderingEvent(_) => {
                WorkerFrameQueueItemRenderingEvent::type_name()
            }
            WebSocketMessage::WorkerFrameQueueItemFinishedEvent(_) => {
                WorkerFrameQueueItemFinishedEvent::type_name()
            }
            WebSocketMessage::MasterHeartbeatRequest(_) => MasterHeartbeatRequest::type_name(),
            WebSocketMessage::WorkerHeartbeatResponse(_) => WorkerHeartbeatResponse::type_name(),
            WebSocketMessage::MasterJobStartedEvent(_) => MasterJobStartedEvent::type_name(),
            WebSocketMessage::MasterJobFinishedRequest(_) => MasterJobFinishedRequest::type_name(),
            WebSocketMessage::WorkerJobFinishedResponse(_) => WorkerJobFinishedResponse::type_name(),
        }
    }
}
