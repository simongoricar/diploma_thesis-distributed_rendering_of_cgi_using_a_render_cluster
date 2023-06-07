pub mod queue;
pub mod receiver;
pub mod requester;
pub mod sender;


use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use log::debug;
use miette::{miette, Context, IntoDiagnostic, Result};
use shared::cancellation::CancellationToken;
use shared::jobs::BlenderJob;
use shared::logger::Logger;
use shared::messages::handshake::{MasterHandshakeAcknowledgement, MasterHandshakeRequest};
use shared::messages::heartbeat::MasterHeartbeatRequest;
use shared::messages::queue::{FrameQueueAddResult, WorkerFrameQueueItemFinishedEvent};
use shared::messages::SenderHandle;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio_tungstenite::accept_async;

use crate::cluster::state::ClusterManagerState;
use crate::connection::queue::{FrameOnWorker, WorkerQueue};
use crate::connection::receiver::WorkerReceiver;
use crate::connection::requester::WorkerRequester;
use crate::connection::sender::WorkerSender;

/// State of the WebSocket connection with the worker.
pub enum WorkerConnectionState {
    PendingHandshake,
    Connected,
}

/// Worker abstraction from the viewpoint of the master server.
pub struct Worker {
    /// IP address and port of the worker.
    pub address: SocketAddr,

    /// Logger instance (so we can display logs with IP address, port and other context).
    pub logger: Arc<Logger>,

    /// Reference to the `ClusterManager`'s state.
    pub cluster_state: Arc<ClusterManagerState>,

    /// Status of the WebSocket connection with the worker.
    pub connection_state: WorkerConnectionState,

    pub sender: WorkerSender,

    /// Event dispatcher for this worker. After the handshake is complete, this handles
    /// all of the incoming requests/responses/events and distributes them through
    /// distinct async channels.  
    pub receiver: Arc<WorkerReceiver>,

    pub requester: Arc<WorkerRequester>,

    /// Internal queue representation for this worker.
    /// Likely to be slightly off sync with the actual worker (as WebSockets introduce minimal latency).
    pub queue: Arc<Mutex<WorkerQueue>>,

    /// `JoinSet` that contains all the async tasks this worker has running.
    pub connection_tasks: JoinSet<Result<()>>,

    pub heartbeat_cancellation_token: CancellationToken,

    pub cluster_cancellation_token: CancellationToken,
}

impl Worker {
    /// Given an established incoming `TcpStream`, turn the connection into
    /// a WebSocket connection, perform our internal handshake and spawn tasks to run the connection
    /// with this client and handle messages.
    pub async fn new_with_accept_and_handshake(
        stream: TcpStream,
        address: SocketAddr,
        cluster_state: Arc<ClusterManagerState>,
        heartbeat_cancellation_token: CancellationToken,
        cluster_cancellation_token: CancellationToken,
    ) -> Result<Self> {
        let queue = Arc::new(Mutex::new(WorkerQueue::new()));

        let (logger, sender, receiver, requester, connection_tasks) =
            Self::accept_ws_stream_and_initialize_tasks(
                stream,
                queue.clone(),
                cluster_state.clone(),
                heartbeat_cancellation_token.clone(),
                cluster_cancellation_token.clone(),
            )
            .await?;

        Ok(Self {
            address,
            logger,
            cluster_state,
            connection_state: WorkerConnectionState::Connected,
            sender,
            receiver,
            requester,
            queue,
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
    pub async fn queue_frame(&self, job: BlenderJob, frame_index: usize) -> Result<()> {
        let add_result = self
            .requester
            .frame_queue_add_item(job.clone(), frame_index)
            .await?;

        match add_result {
            FrameQueueAddResult::AddedToQueue => {
                self.queue
                    .lock()
                    .await
                    .add(FrameOnWorker::new(job, frame_index));
                Ok(())
            }
            FrameQueueAddResult::Errored { reason } => Err(miette!(
                "Errored on worker when trying to add frame to queue: {}",
                reason
            )),
        }
    }

    /*
     * Private WebSocket accepting, handshaking and other connection code.
     */

    /// Given a `TcpStream`, turn it into a proper WebSocket connection, perform the initial handshake
    /// and spawn tasks to run this connection, including handling incoming requests/responses/events.
    async fn accept_ws_stream_and_initialize_tasks(
        stream: TcpStream,
        queue: Arc<Mutex<WorkerQueue>>,
        cluster_state: Arc<ClusterManagerState>,
        heartbeat_cancellation_token: CancellationToken,
        cluster_cancellation_token: CancellationToken,
    ) -> Result<(
        Arc<Logger>,
        WorkerSender,
        Arc<WorkerReceiver>,
        Arc<WorkerRequester>,
        JoinSet<Result<()>>,
    )> {
        // Initialize logger that will be distributed across this worker instance.
        let address = stream.peer_addr().into_diagnostic()?;
        let logger = Arc::new(Logger::new(format!(
            "[worker|{}:{}]",
            address.ip(),
            address.port()
        )));

        // Upgrades TcpStream to a WebSocket connection.
        logger.debug("Accepting TcpStream.");
        let ws_stream = accept_async(stream)
            .await
            .into_diagnostic()
            .wrap_err_with(|| miette!("Could not accept TcpStream."))?;
        logger.debug("TcpStream accepted.");

        // Splits the stream and initializes unbounded message channels.

        let (ws_sink, ws_stream) = ws_stream.split();

        // The multiple async tasks that run this connection live on this `JoinSet`.
        // Use this `JoinSet` to cancel or join all of the worker tasks.
        let mut worker_connection_future_set: JoinSet<Result<()>> = JoinSet::new();

        let receiver = WorkerReceiver::new(ws_stream, cluster_cancellation_token.clone());
        let receiver_arc = Arc::new(receiver);

        let sender = WorkerSender::new(
            ws_sink,
            cluster_cancellation_token.clone(),
            logger.clone(),
        );

        // Perform our internal WebSocket handshake that exchanges, among other things, server and worker versions.
        Self::perform_handshake(logger.clone(), &sender, receiver_arc.clone()).await?;

        let requester = WorkerRequester::new(sender.sender_handle(), receiver_arc.clone());
        let requester_arc = Arc::new(requester);

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
            logger.clone(),
            receiver_arc.clone(),
            queue,
            cluster_state,
            cluster_cancellation_token.clone(),
        ));

        logger.info("Worker connection fully established.");

        Ok((
            logger,
            sender,
            receiver_arc,
            requester_arc,
            worker_connection_future_set,
        ))
    }

    /// Perform our internal handshake over the given WebSocket.
    /// This handshake consists of three messages:
    ///  1. The master server must send a "handshake request".
    ///  2. The worker must respond with a "handshake response".
    ///  3. The master server must confirm with a "handshake response acknowledgement".
    ///
    /// After these three steps, the connection is considered to be established.
    async fn perform_handshake(
        logger: Arc<Logger>,
        sender: &WorkerSender,
        receiver: Arc<WorkerReceiver>,
    ) -> Result<()> {
        let sender_handle = sender.sender_handle();

        logger.debug("Sending handshake request.");
        sender_handle
            .send_message(MasterHandshakeRequest::new("1.0.0"))
            .await?;

        let handshake_response = receiver
            .wait_for_handshake_response(None)
            .await
            .wrap_err_with(|| miette!("Failed to receive handshake response."))?;

        logger.info(format!(
            "Got handshake response from worker. worker_version={}, sending acknowledgement.",
            handshake_response.worker_version,
        ));

        sender_handle
            .send_message(MasterHandshakeAcknowledgement::new(true))
            .await?;

        logger.info("Handshake has been successfully completed.");

        Ok(())
    }

    /// Waits for incoming messages and reacts according to their contents
    /// (e.g. a "frame finished" event will update our internal queue to reflect that).
    async fn manage_incoming_messages(
        logger: Arc<Logger>,
        event_dispatcher: Arc<WorkerReceiver>,
        queue: Arc<Mutex<WorkerQueue>>,
        cluster_state: Arc<ClusterManagerState>,
        cluster_cancellation_token: CancellationToken,
    ) -> Result<()> {
        logger.debug("Running task loop: handling incoming messages.");

        let mut queue_item_finished_receiver =
            event_dispatcher.frame_queue_item_finished_event_receiver();

        loop {
            tokio::select! {
                notification = queue_item_finished_receiver.recv() => {
                    let notification: WorkerFrameQueueItemFinishedEvent = notification.into_diagnostic()?;

                    debug!(
                        "Received: Queue item finished, frame: {}",
                        notification.frame_index
                    );

                    Self::mark_frame_as_finished(
                        notification.job_name,
                        notification.frame_index,
                        logger.clone(),
                        queue.clone(),
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
        logger: Arc<Logger>,
        event_dispatcher: Arc<WorkerReceiver>,
        sender_handle: SenderHandle,
        heartbeat_cancellation_token: CancellationToken,
        cluster_cancellation_token: CancellationToken,
    ) -> Result<()> {
        logger.debug("Running task loop: heartbeats.");

        let mut last_heartbeat_time = Instant::now();

        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;

            if heartbeat_cancellation_token.is_cancelled() {
                logger.trace("Stopping heartbeat task (heartbeat cancelled).");
                break;
            }
            if cluster_cancellation_token.is_cancelled() {
                logger.trace("Stopping heartbeat task (cluster stopping).");
                break;
            }

            if last_heartbeat_time.elapsed() > Duration::from_secs(10) {
                logger.debug("Sending heartbeat request to worker.");
                sender_handle
                    .send_message(MasterHeartbeatRequest::new())
                    .await?;

                let time_heartbeat_start = Instant::now();

                event_dispatcher
                    .wait_for_heartbeat_response(None)
                    .await
                    .wrap_err_with(|| miette!("Worker did not respond to heartbeat."))?;

                let time_heartbeat_latency = time_heartbeat_start.elapsed();

                logger.debug(format!(
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

    /// Updates the internal `Worker` and slightly more global `ClusterManagerState` state
    /// to reflect the worker having finished its queued frame.
    async fn mark_frame_as_finished(
        job_name: String,
        frame_index: usize,
        logger: Arc<Logger>,
        queue: Arc<Mutex<WorkerQueue>>,
        cluster_state: Arc<ClusterManagerState>,
    ) -> Result<()> {
        {
            let mut locked_queue = queue.lock().await;
            logger.trace("Removing frame from master's internal worker state.");
            locked_queue.remove(job_name, frame_index)?
        }

        {
            logger.trace("Marking frame as finished in master's full state.");
            cluster_state.mark_frame_as_finished(frame_index).await?;
        }

        Ok(())
    }
}
