use std::time::SystemTime;

use miette::{miette, Report};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use serde_with::TimestampSecondsWithFrac;

use crate::messages::traits::Message;
use crate::messages::WebSocketMessage;

pub static MASTER_HEARTBEAT_REQUEST_TYPE_NAME: &str = "request_heartbeat";

#[serde_as]
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MasterHeartbeatRequest {
    #[serde_as(as = "TimestampSecondsWithFrac<f64>")]
    pub request_time: SystemTime,
}

impl MasterHeartbeatRequest {
    pub fn new_now() -> Self {
        Self {
            request_time: SystemTime::now(),
        }
    }
}

impl Message for MasterHeartbeatRequest {
    fn type_name() -> &'static str {
        MASTER_HEARTBEAT_REQUEST_TYPE_NAME
    }
}
impl From<MasterHeartbeatRequest> for WebSocketMessage {
    fn from(value: MasterHeartbeatRequest) -> Self {
        WebSocketMessage::MasterHeartbeatRequest(value)
    }
}
impl TryFrom<WebSocketMessage> for MasterHeartbeatRequest {
    type Error = Report;

    fn try_from(value: WebSocketMessage) -> Result<Self, Self::Error> {
        match value {
            WebSocketMessage::MasterHeartbeatRequest(request) => Ok(request),
            _ => Err(miette!("Invalid message type!")),
        }
    }
}



pub static WORKER_HEARTBEAT_RESPONSE_TYPE_NAME: &str = "request_heartbeat";

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct WorkerHeartbeatResponse {}

impl WorkerHeartbeatResponse {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {}
    }
}

impl Message for WorkerHeartbeatResponse {
    fn type_name() -> &'static str {
        WORKER_HEARTBEAT_RESPONSE_TYPE_NAME
    }
}
impl From<WorkerHeartbeatResponse> for WebSocketMessage {
    fn from(value: WorkerHeartbeatResponse) -> Self {
        WebSocketMessage::WorkerHeartbeatResponse(value)
    }
}
impl TryFrom<WebSocketMessage> for WorkerHeartbeatResponse {
    type Error = Report;

    fn try_from(value: WebSocketMessage) -> Result<Self, Self::Error> {
        match value {
            WebSocketMessage::WorkerHeartbeatResponse(response) => Ok(response),
            _ => Err(miette!("Invalid message type!")),
        }
    }
}
