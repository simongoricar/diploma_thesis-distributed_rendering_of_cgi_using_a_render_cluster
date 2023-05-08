use serde::{Deserialize, Serialize};

use crate::messages::traits::Message;
use crate::messages::WebSocketMessage;

pub static MASTER_HANDSHAKE_REQUEST_TYPE_NAME: &str = "handshake_request";

#[derive(Serialize, Deserialize)]
pub struct MasterHandshakeRequest {
    pub server_version: String,
}

impl MasterHandshakeRequest {
    pub fn new<S: Into<String>>(server_version: S) -> Self {
        Self {
            server_version: server_version.into(),
        }
    }
}

impl Message for MasterHandshakeRequest {
    fn type_name() -> &'static str {
        MASTER_HANDSHAKE_REQUEST_TYPE_NAME
    }
}
impl From<MasterHandshakeRequest> for WebSocketMessage {
    fn from(value: MasterHandshakeRequest) -> Self {
        WebSocketMessage::MasterHandshakeRequest(value)
    }
}



pub static WORKER_HANDSHAKE_RESPONSE_TYPE_NAME: &str = "handshake_response";

#[derive(Serialize, Deserialize)]
pub struct WorkerHandshakeResponse {
    pub worker_version: String,
}

impl Message for WorkerHandshakeResponse {
    fn type_name() -> &'static str {
        WORKER_HANDSHAKE_RESPONSE_TYPE_NAME
    }
}
impl From<WorkerHandshakeResponse> for WebSocketMessage {
    fn from(value: WorkerHandshakeResponse) -> Self {
        WebSocketMessage::WorkerHandshakeResponse(value)
    }
}



pub static MASTER_HANDSHAKE_ACK_TYPE_NAME: &str = "handshake_acknowledgement";

#[derive(Serialize, Deserialize)]
pub struct MasterHandshakeAcknowledgement {
    pub ok: bool,
}

impl MasterHandshakeAcknowledgement {
    pub fn new(ok: bool) -> Self {
        Self { ok }
    }
}

impl Message for MasterHandshakeAcknowledgement {
    fn type_name() -> &'static str {
        MASTER_HANDSHAKE_ACK_TYPE_NAME
    }
}
impl From<MasterHandshakeAcknowledgement> for WebSocketMessage {
    fn from(value: MasterHandshakeAcknowledgement) -> Self {
        WebSocketMessage::MasterHandshakeAcknowledgement(value)
    }
}
