use miette::{miette, Report};
use serde::{Deserialize, Serialize};

use crate::messages::traits::Message;
use crate::messages::WebSocketMessage;

#[derive(Serialize, Deserialize, Clone, Debug, Copy, Eq, PartialEq, Hash)]
#[repr(transparent)]
pub struct WorkerID(i64);

impl WorkerID {
    pub fn generate() -> Self {
        let random_worker_id: i64 = rand::random();

        Self(random_worker_id)
    }
}


pub static MASTER_HANDSHAKE_REQUEST_TYPE_NAME: &str = "handshake_request";

#[derive(Serialize, Deserialize, Clone, Debug)]
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
impl TryFrom<WebSocketMessage> for MasterHandshakeRequest {
    type Error = Report;

    fn try_from(value: WebSocketMessage) -> Result<Self, Self::Error> {
        match value {
            WebSocketMessage::MasterHandshakeRequest(request) => Ok(request),
            _ => Err(miette!("Invalid message type!")),
        }
    }
}



#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
pub enum WorkerHandshakeType {
    #[serde(rename = "first-connection")]
    FirstConnection,

    #[serde(rename = "reconnecting")]
    Reconnecting,
}


pub static WORKER_HANDSHAKE_RESPONSE_TYPE_NAME: &str = "handshake_response";

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct WorkerHandshakeResponse {
    pub handshake_type: WorkerHandshakeType,

    pub worker_version: String,

    pub worker_id: WorkerID,
}

impl WorkerHandshakeResponse {
    pub fn new_first_connection<S: Into<String>>(worker_version: S, worker_id: WorkerID) -> Self {
        Self {
            handshake_type: WorkerHandshakeType::FirstConnection,
            worker_version: worker_version.into(),
            worker_id,
        }
    }

    pub fn new_reconnection<S: Into<String>>(worker_version: S, worker_id: WorkerID) -> Self {
        Self {
            handshake_type: WorkerHandshakeType::Reconnecting,
            worker_version: worker_version.into(),
            worker_id,
        }
    }
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
impl TryFrom<WebSocketMessage> for WorkerHandshakeResponse {
    type Error = Report;

    fn try_from(value: WebSocketMessage) -> Result<Self, Self::Error> {
        match value {
            WebSocketMessage::WorkerHandshakeResponse(response) => Ok(response),
            _ => Err(miette!("Invalid message type!")),
        }
    }
}



pub static MASTER_HANDSHAKE_ACK_TYPE_NAME: &str = "handshake_acknowledgement";

#[derive(Serialize, Deserialize, Clone, Debug)]
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
impl TryFrom<WebSocketMessage> for MasterHandshakeAcknowledgement {
    type Error = Report;

    fn try_from(value: WebSocketMessage) -> Result<Self, Self::Error> {
        match value {
            WebSocketMessage::MasterHandshakeAcknowledgement(ack) => Ok(ack),
            _ => Err(miette!("Invalid message type!")),
        }
    }
}
