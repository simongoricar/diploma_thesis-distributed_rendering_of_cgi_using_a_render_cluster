use miette::{miette, Report};
use serde::{Deserialize, Serialize};

use crate::messages::traits::Message;
use crate::messages::utilities::MessageRequestID;
use crate::messages::WebSocketMessage;
use crate::results::worker_trace::WorkerTrace;

pub static MASTER_JOB_STARTED_EVENT_TYPE_NAME: &str = "event_job-started";

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MasterJobStartedEvent {}

impl MasterJobStartedEvent {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {}
    }
}

impl Message for MasterJobStartedEvent {
    fn type_name() -> &'static str {
        MASTER_JOB_STARTED_EVENT_TYPE_NAME
    }
}

impl From<MasterJobStartedEvent> for WebSocketMessage {
    fn from(value: MasterJobStartedEvent) -> Self {
        WebSocketMessage::MasterJobStartedEvent(value)
    }
}

impl TryFrom<WebSocketMessage> for MasterJobStartedEvent {
    type Error = Report;

    fn try_from(value: WebSocketMessage) -> Result<Self, Self::Error> {
        match value {
            WebSocketMessage::MasterJobStartedEvent(event) => Ok(event),
            _ => Err(miette!("Invalid message type!")),
        }
    }
}



pub static MASTER_JOB_FINISHED_REQUEST_TYPE_NAME: &str = "request_job-finished";

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MasterJobFinishedRequest {
    pub message_request_id: MessageRequestID,
}

impl MasterJobFinishedRequest {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            message_request_id: MessageRequestID::generate(),
        }
    }
}

impl Message for MasterJobFinishedRequest {
    #[inline]
    fn type_name() -> &'static str {
        MASTER_JOB_FINISHED_REQUEST_TYPE_NAME
    }
}

impl From<MasterJobFinishedRequest> for WebSocketMessage {
    fn from(value: MasterJobFinishedRequest) -> Self {
        WebSocketMessage::MasterJobFinishedRequest(value)
    }
}

impl TryFrom<WebSocketMessage> for MasterJobFinishedRequest {
    type Error = Report;

    fn try_from(value: WebSocketMessage) -> Result<Self, Self::Error> {
        match value {
            WebSocketMessage::MasterJobFinishedRequest(request) => Ok(request),
            _ => Err(miette!("Invalid message type!")),
        }
    }
}



// TODO Integrate
pub static WORKER_JOB_FINISHED_RESPONSE_TYPE_NAME: &str = "response_job-finished";

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct WorkerJobFinishedResponse {
    pub message_request_context_id: MessageRequestID,

    pub trace: WorkerTrace,
}

impl WorkerJobFinishedResponse {
    pub fn new(request_id: MessageRequestID, trace: WorkerTrace) -> Self {
        Self {
            message_request_context_id: request_id,
            trace,
        }
    }
}

impl Message for WorkerJobFinishedResponse {
    fn type_name() -> &'static str {
        WORKER_JOB_FINISHED_RESPONSE_TYPE_NAME
    }
}

impl From<WorkerJobFinishedResponse> for WebSocketMessage {
    fn from(value: WorkerJobFinishedResponse) -> Self {
        WebSocketMessage::WorkerJobFinishedResponse(value)
    }
}

impl TryFrom<WebSocketMessage> for WorkerJobFinishedResponse {
    type Error = Report;

    fn try_from(value: WebSocketMessage) -> Result<Self, Self::Error> {
        match value {
            WebSocketMessage::WorkerJobFinishedResponse(response) => Ok(response),
            _ => Err(miette!("Invalid message type!")),
        }
    }
}
