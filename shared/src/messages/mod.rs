use futures_channel::mpsc::UnboundedSender;
use miette::{miette, Context, IntoDiagnostic, Result};
use serde::{Deserialize, Serialize};
use tokio_tungstenite::tungstenite;

use crate::messages::handshake::{
    MasterHandshakeAcknowledgement,
    MasterHandshakeRequest,
    WorkerHandshakeResponse,
};
use crate::messages::queue::{
    MasterFrameQueueAddRequest,
    MasterFrameQueueRemoveRequest,
    WorkerFrameQueueItemFinishedNotification,
};

pub mod handshake;
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
    WorkerFrameQueueItemFinishedNotification(
        WorkerFrameQueueItemFinishedNotification,
    ),
}

impl WebSocketMessage {
    pub fn from_websocket_message(
        message: tungstenite::Message,
    ) -> Result<Self> {
        match message {
            tungstenite::Message::Text(string) => {
                let ws_message: WebSocketMessage =
                    serde_json::from_str(&string).into_diagnostic()?;
                Ok(ws_message)
            }
            tungstenite::Message::Binary(_) => {
                todo!(
                    "Not implemented, need to handle binary websocket messages."
                );
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

    pub fn send(
        &self,
        sender: &UnboundedSender<tungstenite::Message>,
    ) -> Result<()> {
        sender
            .unbounded_send(self.to_websocket_message()?)
            .into_diagnostic()
            .wrap_err_with(|| miette!("Could not queue WebSocket message."))?;

        Ok(())
    }
}
