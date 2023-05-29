use serde::{Deserialize, Serialize};

use crate::jobs::BlenderJob;
use crate::messages::traits::Message;
use crate::messages::WebSocketMessage;

pub static MASTER_FRAME_QUEUE_ADD_REQUEST_TYPE_NAME: &str =
    "request_frame-queue_add";

#[derive(Serialize, Deserialize)]
pub struct MasterFrameQueueAddRequest {
    pub job: BlenderJob,
    pub frame_index: usize,
}

impl MasterFrameQueueAddRequest {
    pub fn new(job: BlenderJob, frame_index: usize) -> Self {
        Self { job, frame_index }
    }
}

impl Message for MasterFrameQueueAddRequest {
    fn type_name() -> &'static str {
        MASTER_FRAME_QUEUE_ADD_REQUEST_TYPE_NAME
    }
}
impl From<MasterFrameQueueAddRequest> for WebSocketMessage {
    fn from(value: MasterFrameQueueAddRequest) -> Self {
        WebSocketMessage::MasterFrameQueueAddRequest(value)
    }
}



pub static MASTER_FRAME_QUEUE_REMOVE_REQUEST_TYPE_NAME: &str =
    "request_frame-queue_remove";

#[derive(Serialize, Deserialize)]
pub struct MasterFrameQueueRemoveRequest {
    pub job_name: String,
    pub frame_index: usize,
}

impl MasterFrameQueueRemoveRequest {
    pub fn new(job_name: String, frame_index: usize) -> Self {
        Self {
            job_name,
            frame_index,
        }
    }
}

impl Message for MasterFrameQueueRemoveRequest {
    fn type_name() -> &'static str {
        MASTER_FRAME_QUEUE_REMOVE_REQUEST_TYPE_NAME
    }
}
impl From<MasterFrameQueueRemoveRequest> for WebSocketMessage {
    fn from(value: MasterFrameQueueRemoveRequest) -> Self {
        WebSocketMessage::MasterFrameQueueRemoveRequest(value)
    }
}



pub static WORKER_FRAME_QUEUE_ITEM_FINISHED_NOTIFICATION_TYPE_NAME: &str =
    "notification_frame-queue_item-finished";

#[derive(Serialize, Deserialize)]
pub struct WorkerFrameQueueItemFinishedNotification {
    pub job_name: String,
    pub frame_index: usize,
}

impl WorkerFrameQueueItemFinishedNotification {
    pub fn new(job_name: String, frame_index: usize) -> Self {
        Self {
            job_name,
            frame_index,
        }
    }
}

impl Message for WorkerFrameQueueItemFinishedNotification {
    fn type_name() -> &'static str {
        WORKER_FRAME_QUEUE_ITEM_FINISHED_NOTIFICATION_TYPE_NAME
    }
}
impl From<WorkerFrameQueueItemFinishedNotification> for WebSocketMessage {
    fn from(value: WorkerFrameQueueItemFinishedNotification) -> Self {
        WebSocketMessage::WorkerFrameQueueItemFinishedNotification(value)
    }
}