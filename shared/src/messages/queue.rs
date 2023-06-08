use miette::{miette, Report};
use serde::{Deserialize, Serialize};

use crate::jobs::BlenderJob;
use crate::messages::traits::Message;
use crate::messages::utilities::MessageRequestID;
use crate::messages::WebSocketMessage;

/**
 * Frame queue requests and responses
 **/

pub static MASTER_FRAME_QUEUE_ADD_REQUEST_TYPE_NAME: &str = "request_frame-queue_add";

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MasterFrameQueueAddRequest {
    pub message_request_id: MessageRequestID,

    pub job: BlenderJob,
    pub frame_index: usize,
}

impl MasterFrameQueueAddRequest {
    pub fn new(job: BlenderJob, frame_index: usize) -> Self {
        Self {
            message_request_id: MessageRequestID::generate(),
            job,
            frame_index,
        }
    }
}

impl Message for MasterFrameQueueAddRequest {
    #[inline]
    fn type_name() -> &'static str {
        MASTER_FRAME_QUEUE_ADD_REQUEST_TYPE_NAME
    }
}

impl From<MasterFrameQueueAddRequest> for WebSocketMessage {
    fn from(value: MasterFrameQueueAddRequest) -> Self {
        WebSocketMessage::MasterFrameQueueAddRequest(value)
    }
}

impl TryFrom<WebSocketMessage> for MasterFrameQueueAddRequest {
    type Error = Report;

    fn try_from(value: WebSocketMessage) -> Result<Self, Self::Error> {
        match value {
            WebSocketMessage::MasterFrameQueueAddRequest(request) => Ok(request),
            _ => Err(miette!("Invalid message type!")),
        }
    }
}



pub static WORKER_FRAME_QUEUE_ADD_RESPONSE_TYPE_NAME: &str = "response_frame-queue-add";

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "result")]
pub enum FrameQueueAddResult {
    #[serde(rename = "added-to-queue")]
    AddedToQueue,

    #[serde(rename = "errored")]
    Errored { reason: String },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct WorkerFrameQueueAddResponse {
    pub message_request_context_id: MessageRequestID,

    pub result: FrameQueueAddResult,
}

impl WorkerFrameQueueAddResponse {
    pub fn new_ok(request_id: MessageRequestID) -> Self {
        Self {
            message_request_context_id: request_id,
            result: FrameQueueAddResult::AddedToQueue,
        }
    }

    pub fn new_errored<S: Into<String>>(request_id: MessageRequestID, reason: S) -> Self {
        Self {
            message_request_context_id: request_id,
            result: FrameQueueAddResult::Errored {
                reason: reason.into(),
            },
        }
    }
}

impl Message for WorkerFrameQueueAddResponse {
    fn type_name() -> &'static str {
        WORKER_FRAME_QUEUE_ADD_RESPONSE_TYPE_NAME
    }
}

impl From<WorkerFrameQueueAddResponse> for WebSocketMessage {
    fn from(value: WorkerFrameQueueAddResponse) -> Self {
        WebSocketMessage::WorkerFrameQueueAddResponse(value)
    }
}

impl TryFrom<WebSocketMessage> for WorkerFrameQueueAddResponse {
    type Error = Report;

    fn try_from(value: WebSocketMessage) -> Result<Self, Self::Error> {
        match value {
            WebSocketMessage::WorkerFrameQueueAddResponse(response) => Ok(response),
            _ => Err(miette!("Invalid message type!")),
        }
    }
}



pub static MASTER_FRAME_QUEUE_REMOVE_REQUEST_TYPE_NAME: &str = "request_frame-queue_remove";

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MasterFrameQueueRemoveRequest {
    pub message_request_id: MessageRequestID,

    pub job_name: String,
    pub frame_index: usize,
}

impl MasterFrameQueueRemoveRequest {
    pub fn new(job_name: String, frame_index: usize) -> Self {
        Self {
            message_request_id: MessageRequestID::generate(),
            job_name,
            frame_index,
        }
    }
}

impl Message for MasterFrameQueueRemoveRequest {
    #[inline]
    fn type_name() -> &'static str {
        MASTER_FRAME_QUEUE_REMOVE_REQUEST_TYPE_NAME
    }
}
impl From<MasterFrameQueueRemoveRequest> for WebSocketMessage {
    fn from(value: MasterFrameQueueRemoveRequest) -> Self {
        WebSocketMessage::MasterFrameQueueRemoveRequest(value)
    }
}
impl TryFrom<WebSocketMessage> for MasterFrameQueueRemoveRequest {
    type Error = Report;

    fn try_from(value: WebSocketMessage) -> Result<Self, Self::Error> {
        match value {
            WebSocketMessage::MasterFrameQueueRemoveRequest(request) => Ok(request),
            _ => Err(miette!("Invalid message type!")),
        }
    }
}



pub static WORKER_FRAME_QUEUE_REMOVE_RESPONSE_TYPE_NAME: &str = "response_frame-queue_remove";


#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[serde(tag = "result")]
pub enum FrameQueueRemoveResult {
    #[serde(rename = "removed-from-queue")]
    RemovedFromQueue,

    #[serde(rename = "already-rendering")]
    AlreadyRendering,

    #[serde(rename = "already-finished")]
    AlreadyFinished,

    #[serde(rename = "errored")]
    Errored { reason: String },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct WorkerFrameQueueRemoveResponse {
    pub message_request_context_id: MessageRequestID,

    pub result: FrameQueueRemoveResult,
}

impl WorkerFrameQueueRemoveResponse {
    pub fn new_ok(request_id: MessageRequestID) -> Self {
        Self {
            message_request_context_id: request_id,
            result: FrameQueueRemoveResult::RemovedFromQueue,
        }
    }

    pub fn new_was_already_rendering(request_id: MessageRequestID) -> Self {
        Self {
            message_request_context_id: request_id,
            result: FrameQueueRemoveResult::AlreadyRendering,
        }
    }

    pub fn new_errored<S: Into<String>>(request_id: MessageRequestID, reason: S) -> Self {
        Self {
            message_request_context_id: request_id,
            result: FrameQueueRemoveResult::Errored {
                reason: reason.into(),
            },
        }
    }

    pub fn new_with_result(request_id: MessageRequestID, result: FrameQueueRemoveResult) -> Self {
        Self {
            message_request_context_id: request_id,
            result,
        }
    }
}

impl Message for WorkerFrameQueueRemoveResponse {
    fn type_name() -> &'static str {
        WORKER_FRAME_QUEUE_REMOVE_RESPONSE_TYPE_NAME
    }
}

impl From<WorkerFrameQueueRemoveResponse> for WebSocketMessage {
    fn from(value: WorkerFrameQueueRemoveResponse) -> Self {
        WebSocketMessage::WorkerFrameQueueRemoveResponse(value)
    }
}

impl TryFrom<WebSocketMessage> for WorkerFrameQueueRemoveResponse {
    type Error = Report;

    fn try_from(value: WebSocketMessage) -> Result<Self, Self::Error> {
        match value {
            WebSocketMessage::WorkerFrameQueueRemoveResponse(response) => Ok(response),
            _ => Err(miette!("Invalid message type!")),
        }
    }
}



/**
 * Frame queue events
 **/

pub static WORKER_FRAME_QUEUE_ITEM_RENDERING_EVENT: &str =
    "event_frame-queue_item-started-rendering";

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct WorkerFrameQueueItemRenderingEvent {
    pub job_name: String,
    pub frame_index: usize,
}

impl WorkerFrameQueueItemRenderingEvent {
    pub fn new(job_name: String, frame_index: usize) -> Self {
        Self {
            job_name,
            frame_index,
        }
    }
}

impl Message for WorkerFrameQueueItemRenderingEvent {
    fn type_name() -> &'static str {
        WORKER_FRAME_QUEUE_ITEM_RENDERING_EVENT
    }
}

impl From<WorkerFrameQueueItemRenderingEvent> for WebSocketMessage {
    fn from(value: WorkerFrameQueueItemRenderingEvent) -> Self {
        WebSocketMessage::WorkerFrameQueueItemRenderingEvent(value)
    }
}

impl TryFrom<WebSocketMessage> for WorkerFrameQueueItemRenderingEvent {
    type Error = Report;

    fn try_from(value: WebSocketMessage) -> Result<Self, Self::Error> {
        match value {
            WebSocketMessage::WorkerFrameQueueItemRenderingEvent(event) => Ok(event),
            _ => Err(miette!("Invalid message type!")),
        }
    }
}



pub static WORKER_FRAME_QUEUE_ITEM_FINISHED_EVENT_TYPE_NAME: &str =
    "event_frame-queue_item-finished";

// TODO Integrate this UPDATED event into both the master and the worker.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "result")]
pub enum FrameQueueItemFinishedResult {
    #[serde(rename = "ok")]
    Ok,

    #[serde(rename = "errored")]
    Errored { reason: String },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct WorkerFrameQueueItemFinishedEvent {
    pub job_name: String,

    pub frame_index: usize,

    pub result: FrameQueueItemFinishedResult,
}

impl WorkerFrameQueueItemFinishedEvent {
    pub fn new_ok(job_name: String, frame_index: usize) -> Self {
        Self {
            job_name,
            frame_index,
            result: FrameQueueItemFinishedResult::Ok,
        }
    }

    pub fn new_errored<S: Into<String>>(job_name: String, frame_index: usize, reason: S) -> Self {
        Self {
            job_name,
            frame_index,
            result: FrameQueueItemFinishedResult::Errored {
                reason: reason.into(),
            },
        }
    }
}

impl Message for WorkerFrameQueueItemFinishedEvent {
    #[inline]
    fn type_name() -> &'static str {
        WORKER_FRAME_QUEUE_ITEM_FINISHED_EVENT_TYPE_NAME
    }
}

impl From<WorkerFrameQueueItemFinishedEvent> for WebSocketMessage {
    fn from(value: WorkerFrameQueueItemFinishedEvent) -> Self {
        WebSocketMessage::WorkerFrameQueueItemFinishedEvent(value)
    }
}

impl TryFrom<WebSocketMessage> for WorkerFrameQueueItemFinishedEvent {
    type Error = Report;

    fn try_from(value: WebSocketMessage) -> Result<Self, Self::Error> {
        match value {
            WebSocketMessage::WorkerFrameQueueItemFinishedEvent(event) => Ok(event),
            _ => Err(miette!("Invalid message type!")),
        }
    }
}
