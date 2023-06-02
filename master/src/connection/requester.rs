use std::sync::Arc;

use futures_channel::mpsc::UnboundedSender;
use miette::{miette, Context, Result};
use shared::jobs::BlenderJob;
use shared::messages::queue::{
    FrameQueueAddResult,
    FrameQueueRemoveResult,
    MasterFrameQueueAddRequest,
    MasterFrameQueueRemoveRequest,
};
use shared::messages::traits::IntoWebSocketMessage;
use tokio_tungstenite::tungstenite::Message;

use crate::connection::event_dispatcher::WorkerEventDispatcher;

pub struct WorkerRequester {
    sender_channel: Arc<UnboundedSender<Message>>,
    event_dispatcher: Arc<WorkerEventDispatcher>,
}

impl WorkerRequester {
    pub fn new(
        sender_channel: Arc<UnboundedSender<Message>>,
        event_dispatcher: Arc<WorkerEventDispatcher>,
    ) -> Self {
        Self {
            sender_channel,
            event_dispatcher,
        }
    }

    /*
     * Request dispatching methods that wait for the worker to respond
     */

    pub async fn frame_queue_add_item(
        &self,
        job: BlenderJob,
        frame_index: usize,
    ) -> Result<FrameQueueAddResult> {
        let request = MasterFrameQueueAddRequest::new(job, frame_index);
        let request_id = request.message_request_id;

        request
            .into_ws_message()
            .send(&self.sender_channel)
            .wrap_err_with(|| miette!("Could not send frame queue add request."))?;

        let response = self
            .event_dispatcher
            .wait_for_message_with_predicate(
                self.event_dispatcher
                    .frame_queue_item_add_response_receiver(),
                None,
                |response| response.message_request_context_id == request_id,
            )
            .await
            .wrap_err_with(|| miette!("Could not receive frame queue add response."))?;

        Ok(response.result)
    }

    pub async fn frame_queue_remove_item(
        &self,
        job_name: String,
        frame_index: usize,
    ) -> Result<FrameQueueRemoveResult> {
        let request = MasterFrameQueueRemoveRequest::new(job_name, frame_index);
        let request_id = request.message_request_id;

        request
            .into_ws_message()
            .send(&self.sender_channel)
            .wrap_err_with(|| miette!("Could not send frame queue remove request."))?;

        let response = self
            .event_dispatcher
            .wait_for_message_with_predicate(
                self.event_dispatcher
                    .frame_queue_item_remove_response_receiver(),
                None,
                |response| response.message_request_context_id == request_id,
            )
            .await
            .wrap_err_with(|| miette!("Could not receive frame queue remove response."))?;

        Ok(response.result)
    }
}
