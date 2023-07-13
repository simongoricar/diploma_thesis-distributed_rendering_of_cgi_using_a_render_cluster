pub mod queue;
pub mod receiver;
pub mod requester;
pub mod sender;
pub mod worker_logger;


use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use miette::{miette, Context, IntoDiagnostic, Result};
use shared::cancellation::CancellationToken;
use shared::jobs::BlenderJob;
use shared::messages::handshake::WorkerID;
use shared::messages::heartbeat::MasterHeartbeatRequest;
use shared::messages::queue::{
    FrameQueueAddResult,
    FrameQueueRemoveResult,
    WorkerFrameQueueItemFinishedEvent,
    WorkerFrameQueueItemRenderingEvent,
};
use shared::messages::SenderHandle;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tracing::debug;
use worker_logger::WorkerLogger;

use crate::cluster::state::ClusterManagerState;
use crate::cluster::ReconnectableClientConnection;
use crate::connection::queue::{FrameOnWorker, WorkerQueue};
use crate::connection::receiver::WorkerReceiver;
use crate::connection::requester::WorkerRequester;
use crate::connection::sender::WorkerSender;

const HEARTBEAT_TASK_CHECK_INTERVAL: Duration = Duration::from_secs(2);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

// TODO Implement swappable client websocket that is then passed to the Worker
//      and which the senders and receiver use instead of the raw websocket.


/// Worker abstraction from the viewpoint of the master server.
pub struct Worker {
    pub id: WorkerID,

    pub logger: WorkerLogger,

    /// Reference to the `ClusterManager` state.
    pub cluster_state: Arc<ClusterManagerState>,

    pub connection: Arc<ReconnectableClientConnection>,

    pub sender: WorkerSender,

    /// Event dispatcher for this worker. After the handshake is complete, this handles
    /// all of the incoming requests/responses/events and distributes them through
    /// distinct async channels.  
    pub receiver: Arc<WorkerReceiver>,

    pub requester: Arc<WorkerRequester>,

    /// Internal queue representation for this worker.
    /// Likely to be slightly off sync with the actual worker (as WebSockets introduce minimal latency).
    pub queue: Arc<Mutex<WorkerQueue>>,

    pub queue_size: Arc<AtomicUsize>,

    /// `JoinSet` that contains all the async tasks this worker has running.
    pub connection_tasks: JoinSet<Result<()>>,

    pub heartbeat_cancellation_token: CancellationToken,

    pub cluster_cancellation_token: CancellationToken,
}

impl Worker {
    /// Given an established incoming WebSocket connection that has already established a handshake,
    /// spawn tasks to run the connection with this client and handle incoming messages.
    pub async fn initialize_from_handshaked_connection(
        connection: Arc<ReconnectableClientConnection>,
        cluster_state: Arc<ClusterManagerState>,
        heartbeat_cancellation_token: CancellationToken,
        cluster_cancellation_token: CancellationToken,
    ) -> Result<Self> {
        let worker_id = connection.worker_id;
        let logger = WorkerLogger::new(connection.clone());

        let (queue, queue_size) = WorkerQueue::new();
        let queue_arc = Arc::new(Mutex::new(queue));

        let (sender, receiver, requester, connection_tasks) =
            Self::accept_ws_stream_and_initialize_tasks(
                connection.clone(),
                queue_arc.clone(),
                cluster_state.clone(),
                logger.clone(),
                heartbeat_cancellation_token.clone(),
                cluster_cancellation_token.clone(),
            )
            .await?;

        Ok(Self {
            id: worker_id,
            logger,
            cluster_state,
            connection,
            sender,
            receiver,
            requester,
            queue: queue_arc,
            queue_size,
            connection_tasks,
            heartbeat_cancellation_token,
            cluster_cancellation_token,
        })
    }

    pub async fn join(mut self) -> Result<()> {
        drop(self.requester);

        while !self.connection_tasks.is_empty() {
            self.connection_tasks.join_next().await;
        }

        let event_dispatcher = Arc::try_unwrap(self.receiver)
            .map_err(|_| miette!("Could not unwrap event_dispatcher Arc?!"))?;
        event_dispatcher.join().await?;

        Ok(())
    }

    /*
     * Public methods
     */

    /// Whether the worker has an empty queue
    /// (as far as the representation on the master server is currently concerned, this might have some latency).
    pub async fn has_empty_queue(&self) -> bool {
        self.queue.lock().await.is_empty()
    }

    /// Queue a frame onto the worker. This sends a WebSocket message and waits for the worker response.
    pub async fn queue_frame(
        &self,
        job: BlenderJob,
        frame_index: usize,
        stolen_from: Option<WorkerID>,
    ) -> Result<()> {
        let add_result = self
            .requester
            .frame_queue_add_item(job.clone(), frame_index)
            .await?;

        match add_result {
            FrameQueueAddResult::AddedToQueue => {
                self.queue.lock().await.add(FrameOnWorker::new_queued(
                    job,
                    frame_index,
                    stolen_from,
                ));
                Ok(())
            }
            FrameQueueAddResult::Errored { reason } => Err(miette!(
                "Errored on worker when trying to add frame to queue: {}",
                reason
            )),
        }
    }

    pub async fn unqueue_frame(
        &self,
        job_name: String,
        frame_index: usize,
    ) -> Result<FrameQueueRemoveResult> {
        let remove_result = self
            .requester
            .frame_queue_remove_item(job_name.clone(), frame_index)
            .await?;

        if remove_result == FrameQueueRemoveResult::RemovedFromQueue {
            self.queue.lock().await.remove(job_name, frame_index)?;
        }

        Ok(remove_result)
    }

    /*
     * Private WebSocket accepting, handshaking and other connection code.
     */

    /// Given a `TcpStream`, turn it into a proper WebSocket connection, perform the initial handshake
    /// and spawn tasks to run this connection, including handling incoming requests/responses/events.
    async fn accept_ws_stream_and_initialize_tasks(
        connection: Arc<ReconnectableClientConnection>,
        worker_queue: Arc<Mutex<WorkerQueue>>,
        cluster_state: Arc<ClusterManagerState>,
        logger: WorkerLogger,
        heartbeat_cancellation_token: CancellationToken,
        cluster_cancellation_token: CancellationToken,
    ) -> Result<(
        WorkerSender,
        Arc<WorkerReceiver>,
        Arc<WorkerRequester>,
        JoinSet<Result<()>>,
    )> {
        let receiver = WorkerReceiver::new(
            connection.clone(),
            logger.clone(),
            cluster_cancellation_token.clone(),
        );
        let receiver_arc = Arc::new(receiver);

        let sender = WorkerSender::new(
            connection.clone(),
            cluster_cancellation_token.clone(),
            logger.clone(),
        );

        let requester = WorkerRequester::new(sender.sender_handle(), receiver_arc.clone());
        let requester_arc = Arc::new(requester);


        // The multiple async tasks that run this connection live on this `JoinSet`.
        // Use this `JoinSet` to cancel or join all of the worker tasks.
        let mut worker_connection_future_set: JoinSet<Result<()>> = JoinSet::new();

        // Finally, spawn two final tasks:
        // - one that maintains the heartbeat with the worker (it sends a heartbeat request every ~10 seconds and waits for the response) and
        // - one that, finally, actually manages the incoming messages - things like requests/responses/notifications
        //   and updates the master server's internal worker state if necessary.
        worker_connection_future_set.spawn(Self::maintain_heartbeat(
            logger.clone(),
            receiver_arc.clone(),
            sender.sender_handle(),
            heartbeat_cancellation_token,
            cluster_cancellation_token.clone(),
        ));

        worker_connection_future_set.spawn(Self::manage_incoming_messages(
            connection.worker_id,
            worker_queue,
            logger.clone(),
            receiver_arc.clone(),
            cluster_state,
            cluster_cancellation_token.clone(),
        ));

        logger.info("Connection fully established.");


        Ok((
            sender,
            receiver_arc,
            requester_arc,
            worker_connection_future_set,
        ))
    }

    /// Waits for incoming messages and reacts according to their contents
    /// (e.g. a "frame finished" event will update our internal queue to reflect that).
    async fn manage_incoming_messages(
        worker_id: WorkerID,
        worker_queue: Arc<Mutex<WorkerQueue>>,
        logger: WorkerLogger,
        receiver: Arc<WorkerReceiver>,
        cluster_state: Arc<ClusterManagerState>,
        cluster_cancellation_token: CancellationToken,
    ) -> Result<()> {
        logger.debug("Running task loop: handling incoming messages.");

        let mut queue_item_rendering_receiver = receiver.frame_queue_item_rendering_event_receiver();
        let mut queue_item_finished_receiver = receiver.frame_queue_item_finished_event_receiver();

        loop {
            tokio::select! {
                rendering_event = queue_item_rendering_receiver.recv() => {
                    let rendering_event: WorkerFrameQueueItemRenderingEvent = rendering_event.into_diagnostic()?;

                    debug!(
                        "Received: queue item rendering, frame: {}",
                        rendering_event.frame_index,
                    );

                    Self::mark_frame_as_rendering(
                        rendering_event.job_name,
                        rendering_event.frame_index,
                        worker_id,
                        logger.clone(),
                        worker_queue.clone(),
                        cluster_state.clone(),
                    ).await?;
                },
                finished_event = queue_item_finished_receiver.recv() => {
                    let finished_event: WorkerFrameQueueItemFinishedEvent = finished_event.into_diagnostic()?;

                    debug!(
                        "Received: queue item finished, frame: {}",
                        finished_event.frame_index
                    );

                    Self::mark_frame_as_finished(
                        finished_event.job_name,
                        finished_event.frame_index,
                        logger.clone(),
                        worker_queue.clone(),
                        cluster_state.clone()
                    ).await?;
                },
                _ = tokio::time::sleep(Duration::from_secs(2)) => {
                    if cluster_cancellation_token.is_cancelled() {
                        logger.trace("Stopping incoming messages manager (cluster stopping).");
                        break;
                    }
                },
            }
        }

        Ok(())
    }

    /// Maintain a heartbeat with the worker. Every 10 seconds, a heartbeat request is sent
    /// and the worker is expected to respond to it in at most 5 seconds.
    ///
    /// Runs as long as the async sender channel can be written to
    /// or until the worker doesn't respond to a heartbeat.
    async fn maintain_heartbeat(
        logger: WorkerLogger,
        event_dispatcher: Arc<WorkerReceiver>,
        sender_handle: SenderHandle,
        heartbeat_cancellation_token: CancellationToken,
        cluster_cancellation_token: CancellationToken,
    ) -> Result<()> {
        logger.debug("Running task loop: heartbeats.");

        let mut last_heartbeat_time = Instant::now();

        loop {
            tokio::time::sleep(HEARTBEAT_TASK_CHECK_INTERVAL).await;

            if heartbeat_cancellation_token.is_cancelled() {
                logger.trace("Stopping heartbeat task (heartbeat cancelled).");
                break;
            }
            if cluster_cancellation_token.is_cancelled() {
                logger.trace("Stopping heartbeat task (cluster cancelled).");
                break;
            }

            if last_heartbeat_time.elapsed() > HEARTBEAT_INTERVAL {
                logger.trace("Sending heartbeat request to worker.");
                sender_handle
                    .send_message(MasterHeartbeatRequest::new_now())
                    .await?;

                let time_heartbeat_start = Instant::now();

                event_dispatcher
                    .wait_for_heartbeat_response(None)
                    .await
                    .wrap_err_with(|| miette!("Worker did not respond to heartbeat."))?;

                let time_heartbeat_latency = time_heartbeat_start.elapsed();

                logger.trace(format!(
                    "Worker responded to heartbeat request in {:.4} seconds.",
                    time_heartbeat_latency.as_secs_f64(),
                ));

                last_heartbeat_time = Instant::now();
            }
        }

        Ok(())
    }

    /*
     * Internal state management methods
     */

    async fn mark_frame_as_rendering(
        job_name: String,
        frame_index: usize,
        worker_id: WorkerID,
        logger: WorkerLogger,
        worker_queue: Arc<Mutex<WorkerQueue>>,
        cluster_state: Arc<ClusterManagerState>,
    ) -> Result<()> {
        {
            let mut locked_queue = worker_queue.lock().await;
            logger.trace("Marking frame as rendering in master's worker queue state.");
            locked_queue.set_frame_rendering(job_name, frame_index)?;
        }

        logger.trace("Marking frame as rendering in master's full state.");
        cluster_state
            .mark_frame_as_rendering_on_worker(worker_id, frame_index)
            .await?;

        Ok(())
    }

    /// Updates the internal `Worker` and slightly more global `ClusterManagerState` state
    /// to reflect the worker having finished its queued frame.
    async fn mark_frame_as_finished(
        job_name: String,
        frame_index: usize,
        logger: WorkerLogger,
        worker_queue: Arc<Mutex<WorkerQueue>>,
        cluster_state: Arc<ClusterManagerState>,
    ) -> Result<()> {
        {
            let mut locked_queue = worker_queue.lock().await;
            logger.trace("Removing frame from master's internal worker state.");
            locked_queue.remove(job_name, frame_index)?
        }

        logger.trace("Marking frame as finished in master's full state.");
        cluster_state.mark_frame_as_finished(frame_index).await?;

        Ok(())
    }
}
