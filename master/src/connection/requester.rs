use std::sync::Arc;
use std::time::Duration;

use miette::{miette, Context, Result};
use shared::jobs::BlenderJob;
use shared::messages::job::MasterJobFinishedRequest;
use shared::messages::queue::{
    FrameQueueAddResult,
    FrameQueueRemoveResult,
    MasterFrameQueueAddRequest,
    MasterFrameQueueRemoveRequest,
};
use shared::messages::SenderHandle;
use shared::results::worker_trace::WorkerTrace;

use crate::connection::receiver::WorkerReceiver;

pub struct WorkerRequester {
    sender_handle: SenderHandle,
    event_dispatcher: Arc<WorkerReceiver>,
}

impl WorkerRequester {
    pub fn new(sender_handle: SenderHandle, event_dispatcher: Arc<WorkerReceiver>) -> Self {
        Self {
            sender_handle,
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

        self.sender_handle.send_message(request).await?;

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

        self.sender_handle.send_message(request).await?;

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

    pub async fn finish_job_and_get_trace(&self) -> Result<WorkerTrace> {
        let request = MasterJobFinishedRequest::new();
        let request_id = request.message_request_id;

        self.sender_handle.send_message(request).await?;

        let response = self
            .event_dispatcher
            .wait_for_message_with_predicate(
                self.event_dispatcher.job_finished_response_receiver(),
                Some(Duration::from_secs(20)),
                |response| response.message_request_context_id == request_id,
            )
            .await
            .wrap_err_with(|| miette!("Could not receive job finished response."))?;

        Ok(response.trace)
    }
}
