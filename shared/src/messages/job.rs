use miette::{miette, Report};
use serde::{Deserialize, Serialize};

use crate::messages::traits::Message;
use crate::messages::WebSocketMessage;

pub static MASTER_JOB_FINISHED_EVENT_TYPE_NAME: &str = "event_job-finished";

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MasterJobFinishedEvent {}

impl MasterJobFinishedEvent {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {}
    }
}

impl Message for MasterJobFinishedEvent {
    fn type_name() -> &'static str {
        MASTER_JOB_FINISHED_EVENT_TYPE_NAME
    }
}

impl From<MasterJobFinishedEvent> for WebSocketMessage {
    fn from(value: MasterJobFinishedEvent) -> Self {
        WebSocketMessage::MasterJobFinishedEvent(value)
    }
}

impl TryFrom<WebSocketMessage> for MasterJobFinishedEvent {
    type Error = Report;

    fn try_from(value: WebSocketMessage) -> Result<Self, Self::Error> {
        match value {
            WebSocketMessage::MasterJobFinishedEvent(event) => Ok(event),
            _ => Err(miette!("Invalid message type!")),
        }
    }
}
