use serde::{Deserialize, Serialize};

use crate::messages::traits::Message;
use crate::messages::WebSocketMessage;

pub static MASTER_HEARTBEAT_REQUEST_TYPE_NAME: &str = "request_heartbeat";

#[derive(Serialize, Deserialize)]
pub struct MasterHeartbeatRequest {}

impl MasterHeartbeatRequest {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {}
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



pub static WORKER_HEARTBEAT_RESPONSE_TYPE_NAME: &str = "request_heartbeat";

#[derive(Serialize, Deserialize)]
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
