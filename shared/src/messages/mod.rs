use futures_channel::mpsc::UnboundedSender;
use miette::{miette, Context, IntoDiagnostic, Result};
use serde::{Deserialize, Serialize};
use tokio_tungstenite::tungstenite;

use crate::messages::handshake::{
    MasterHandshakeAcknowledgement,
    MasterHandshakeRequest,
    WorkerHandshakeResponse,
};
use crate::messages::heartbeat::{MasterHeartbeatRequest, WorkerHeartbeatResponse};
use crate::messages::queue::{
    MasterFrameQueueAddRequest,
    MasterFrameQueueRemoveRequest,
    WorkerFrameQueueItemFinishedNotification,
};
use crate::messages::traits::Message;

pub mod handshake;
pub mod heartbeat;
pub mod queue;
pub mod traits;


#[derive(Serialize, Deserialize)]
#[serde(tag = "message_type", content = "payload")]
pub enum WebSocketMessage {
    #[serde(rename = "handshake_request")]
    MasterHandshakeRequest(MasterHandshakeRequest),

    #[serde(rename = "handshake_response")]
    WorkerHandshakeResponse(WorkerHandshakeResponse),

    #[serde(rename = "handshake_acknowledgement")]
    MasterHandshakeAcknowledgement(MasterHandshakeAcknowledgement),

    #[serde(rename = "request_frame-queue_add")]
    MasterFrameQueueAddRequest(MasterFrameQueueAddRequest),

    #[serde(rename = "request_frame-queue_remove")]
    MasterFrameQueueRemoveRequest(MasterFrameQueueRemoveRequest),

    #[serde(rename = "request_frame-queue_remove")]
    WorkerFrameQueueItemFinishedNotification(WorkerFrameQueueItemFinishedNotification),

    #[serde(rename = "request_heartbeat")]
    MasterHeartbeatRequest(MasterHeartbeatRequest),

    #[serde(rename = "response_heartbeat")]
    WorkerHeartbeatResponse(WorkerHeartbeatResponse),
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
                todo!("Not implemented, need to handle binary websocket messages.");
            }
            _ => Err(miette!("Invalid WebSocket message type.")),
        }
    }

    pub fn to_websocket_message(&self) -> Result<tungstenite::Message> {
        let serialized_string = serde_json::to_string(self)
            .into_diagnostic()
            .wrap_err_with(|| miette!("Could not serialize to JSON."))?;

        Ok(tungstenite::Message::Text(serialized_string))
    }

    pub fn send(&self, sender: &UnboundedSender<tungstenite::Message>) -> Result<()> {
        sender
            .unbounded_send(self.to_websocket_message()?)
            .into_diagnostic()
            .wrap_err_with(|| miette!("Could not queue WebSocket message."))?;

        Ok(())
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
            WebSocketMessage::MasterFrameQueueRemoveRequest(_) => {
                MasterFrameQueueRemoveRequest::type_name()
            }
            WebSocketMessage::WorkerFrameQueueItemFinishedNotification(_) => {
                WorkerFrameQueueItemFinishedNotification::type_name()
            }
            WebSocketMessage::MasterHeartbeatRequest(_) => MasterHeartbeatRequest::type_name(),
            WebSocketMessage::WorkerHeartbeatResponse(_) => WorkerHeartbeatResponse::type_name(),
        }
    }
}
