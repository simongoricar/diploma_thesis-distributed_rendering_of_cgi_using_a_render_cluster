pub mod receiver;
pub mod sender;

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use futures_util::StreamExt;
use log::{debug, info, trace, warn};
use miette::{miette, Context, IntoDiagnostic, Result};
use shared::cancellation::CancellationToken;
use shared::messages::handshake::WorkerHandshakeResponse;
use shared::messages::heartbeat::WorkerHeartbeatResponse;
use shared::messages::job::{
    MasterJobFinishedRequest,
    MasterJobStartedEvent,
    WorkerJobFinishedResponse,
};
use shared::messages::queue::{
    MasterFrameQueueAddRequest,
    MasterFrameQueueRemoveRequest,
    WorkerFrameQueueAddResponse,
    WorkerFrameQueueRemoveResponse,
};
use shared::messages::SenderHandle;
use shared::results::worker_trace::WorkerTraceBuilder;
use tokio::net::TcpStream;
use tokio_tungstenite::client_async;

use crate::connection::receiver::MasterReceiver;
use crate::connection::sender::MasterSender;
use crate::rendering::queue::WorkerAutomaticQueue;
use crate::rendering::runner::BlenderJobRunner;

/// Worker instance. Manages the connection with the master server, receives requests
/// and performs the rendering as instructed.
pub struct Worker {}

impl Worker {
    /// This connects and performs the initial handshake with the master server, then runs the worker,
    /// spawning several async tasks that receive, send and parse messages.
    ///
    /// Whenever a frame is queued, we render it and respond with the item finished event
    /// (but only one frame at a time).
    pub async fn connect_and_run_to_job_completion(
        master_server_url: &str,
        blender_runner: BlenderJobRunner,
        tracer: WorkerTraceBuilder,
    ) -> Result<()> {
        let tcp_connection = Self::establish_tcp_connection_with_exponential_backoff(
            master_server_url,
            12,
            2f64,
            30f64,
        )
        .await
        .wrap_err_with(|| miette!("Could not connect to master server."))?;

        let (stream, _) = client_async(
            format!("ws://{}", master_server_url),
            tcp_connection,
        )
        .await
        .into_diagnostic()?;

        // Split the WebSocket stream and prepare async channels, but don't actually perform the handshake and other stuff yet.
        // See `run_forever` for running the worker.
        let (ws_sink, ws_stream) = stream.split();

        let global_cancellation_token = CancellationToken::new();

        let sender = MasterSender::new(ws_sink, global_cancellation_token.clone());

        let receiver = MasterReceiver::new(ws_stream, global_cancellation_token.clone());

        Self::perform_handshake(&sender, &receiver)
            .await
            .wrap_err_with(|| miette!("Failed to perform handshake."))?;

        let receiver_arc = Arc::new(receiver);

        let heartbeat_task_handle = tokio::spawn(Self::respond_to_heartbeats(
            receiver_arc.clone(),
            sender.sender_handle(),
            global_cancellation_token.clone(),
            tracer.clone(),
        ));

        let manager_task_handle = tokio::spawn(Self::manage_incoming_messages(
            blender_runner,
            receiver_arc.clone(),
            sender.sender_handle(),
            tracer,
            global_cancellation_token.clone(),
        ));

        info!("Connection fully established and async tasks are running.");

        let _ = manager_task_handle.await.into_diagnostic()?;
        let _ = heartbeat_task_handle.await.into_diagnostic()?;

        let _ = sender.join().await;

        let receiver =
            Arc::try_unwrap(receiver_arc).expect("BUG: Something is holding receiver_arc!");
        let _ = receiver.join().await;

        Ok(())
    }

    async fn establish_tcp_connection_with_exponential_backoff(
        server_url: &str,
        max_retries: usize,
        exp_base: f64,
        backoff_max_seconds: f64,
    ) -> Result<TcpStream> {
        let mut retries: usize = 0;

        while retries < max_retries {
            debug!(
                "Trying to establish connection with master server at {}.",
                server_url
            );
            let tcp_stream = TcpStream::connect(server_url).await;

            if let Ok(stream) = tcp_stream {
                return Ok(stream);
            }

            let mut retry_in = Duration::from_secs_f64(exp_base.powi(retries as i32));
            if retry_in.as_secs_f64() > backoff_max_seconds {
                retry_in = Duration::from_secs_f64(backoff_max_seconds);
            }

            warn!(
                "Could not establish connection (try {}), retrying in {:.2} seconds.",
                retries,
                retry_in.as_secs_f64()
            );
            retries += 1;

            tokio::time::sleep(retry_in).await;
        }

        Err(miette!("Failed to establish connection."))
    }

    /// Performs our internal WebSocket handshake
    /// (master handshake request, worker response, master acknowledgement).
    async fn perform_handshake(sender: &MasterSender, receiver: &MasterReceiver) -> Result<()> {
        let sender_handle = sender.sender_handle();
        let handshake_request_receiver = receiver.handshake_request_receiver();
        let handshake_ack_receiver = receiver.handshake_ack_receiver();

        info!("Waiting for handshake request from master server.");

        let handshake_request = receiver
            .wait_for_handshake_request(Some(handshake_request_receiver), None)
            .await
            .wrap_err_with(|| miette!("Failed to receive handshake request."))?;

        info!(
            "Got handshake request from master server (server_version={}), sending response.",
            handshake_request.server_version
        );

        sender_handle
            .send_message(WorkerHandshakeResponse::new("1.0.0"))
            .await?;

        debug!("Sent handshake response, waiting for acknowledgement.");

        let handshake_ack = receiver
            .wait_for_handshake_ack(Some(handshake_ack_receiver), None)
            .await
            .wrap_err_with(|| miette!("Failed to receive handshake ack."))?;

        if !handshake_ack.ok {
            return Err(miette!(
                "Master server rejected us (acknowledgement not ok)."
            ));
        }

        info!("Handshake finished, server fully connected.");
        Ok(())
    }

    /// Listen for heartbeat requests from the master server and respond to them.
    ///
    /// Runs as long as the async sender channel can be written to.
    async fn respond_to_heartbeats(
        receiver: Arc<MasterReceiver>,
        sender_handle: SenderHandle,
        cancellation_token: CancellationToken,
        tracer: WorkerTraceBuilder,
    ) -> Result<()> {
        debug!("Running task loop: responding to heartbeats.");

        let mut heartbeat_request_receiver = receiver.heartbeat_request_receiver();

        loop {
            let heartbeat_request_result = tokio::time::timeout(
                Duration::from_secs(2),
                heartbeat_request_receiver.recv(),
            )
            .await;

            if cancellation_token.is_cancelled() {
                debug!("Stopping heartbeat responder task (worker stopping).");
                break;
            }

            let (heartbeat_request, request_received_time) = match heartbeat_request_result {
                Ok(potential_request) => {
                    potential_request.into_diagnostic().wrap_err_with(|| {
                        miette!("Failed to receive heartbeat request through channel.")
                    })?
                }
                // Simply wait two more seconds for the next message
                // (this Err happens when we time out on waiting for the next request).
                Err(_) => {
                    continue;
                }
            };

            tracer
                .trace_new_ping(
                    heartbeat_request.request_time,
                    request_received_time,
                )
                .await;

            debug!("Master server sent heartbeat request, responding.");

            sender_handle
                .send_message(WorkerHeartbeatResponse::new())
                .await
                .wrap_err_with(|| {
                    miette!("Could not send heartbeat response through sender channel.")
                })?;
        }

        Ok(())
    }

    /// Waits for incoming requests and reacts to them
    /// (e.g. a "add frame to queue" request will update our frame queue and,
    /// if previously idle, start rendering).
    ///
    /// Runs as long as the event dispatcher's `Receiver`s can be read from.
    async fn manage_incoming_messages(
        blender_runner: BlenderJobRunner,
        receiver: Arc<MasterReceiver>,
        sender_handle: SenderHandle,
        tracer: WorkerTraceBuilder,
        global_cancellation_token: CancellationToken,
    ) -> Result<()> {
        debug!("Running task loop: handling incoming messages.");

        let frame_queue = WorkerAutomaticQueue::initialize(
            blender_runner,
            sender_handle.clone(),
            global_cancellation_token.clone(),
        );

        let mut queue_add_request_receiver = receiver.frame_queue_add_request_receiver();
        let mut queue_remove_request_receiver = receiver.frame_queue_remove_request_receiver();
        let mut job_started_event_receiver = receiver.job_started_event_receiver();
        let mut job_finished_request_receiver = receiver.job_finished_request_receiver();

        loop {
            tokio::select! {
                queue_add_request = queue_add_request_receiver.recv() => {
                    let queue_add_request: MasterFrameQueueAddRequest = queue_add_request.into_diagnostic()?;

                    debug!(
                        "Received: Frame queue add request, frame {}",
                        queue_add_request.frame_index
                    );

                    frame_queue.queue_frame(
                        queue_add_request.job,
                        queue_add_request.frame_index
                    ).await;

                    tracer.trace_new_frame_queued().await;

                    sender_handle.send_message(WorkerFrameQueueAddResponse::new_ok(queue_add_request.message_request_id))
                        .await
                        .wrap_err_with(|| miette!("Could not send frame add response."))?;
                },
                queue_remove_request = queue_remove_request_receiver.recv() => {
                    let queue_remove_request: MasterFrameQueueRemoveRequest = queue_remove_request.into_diagnostic()?;

                    debug!(
                        "Received: Frame queue remove request, frame {}",
                        queue_remove_request.frame_index
                    );

                    let remove_result = frame_queue.unqueue_frame(
                        queue_remove_request.job_name,
                        queue_remove_request.frame_index
                    ).await;

                    tracer.trace_frame_stolen_from_queue().await;

                    debug!("Frame removal result: {remove_result:?}, responding.");

                    sender_handle.send_message(
                        WorkerFrameQueueRemoveResponse::new_with_result(
                            queue_remove_request.message_request_id,
                            remove_result,
                        )
                    )
                        .await
                        .wrap_err_with(|| miette!("Could not send frame remove response."))?;
                },
                job_started_event = job_started_event_receiver.recv() => {
                    let _: MasterJobStartedEvent = job_started_event.into_diagnostic()?;

                    info!("Master server signalled job start.");
                    tracer.set_job_start_time(SystemTime::now()).await;
                },
                job_finished_request = job_finished_request_receiver.recv() => {
                    let request: MasterJobFinishedRequest = job_finished_request.into_diagnostic()?;

                    info!("Master server signalled job finish - sending trace and stopping.");
                    tracer.set_job_finish_time(SystemTime::now()).await;

                    let trace = tracer
                        .build()
                        .await
                        .wrap_err_with(|| miette!("Could not build worker trace."))?;

                    sender_handle.send_message(
                        WorkerJobFinishedResponse::new(
                            request.message_request_id,
                            trace
                        )
                    )
                        .await
                        .wrap_err_with(|| miette!("Could not send job finished response."))?;

                    // Cancel the entire worker set of tasks.
                    debug!("Stopping incoming messages manager (worker stopping).");
                    global_cancellation_token.cancel();

                    break;
                },
                _ = tokio::time::sleep(Duration::from_secs(2)) => {
                    if global_cancellation_token.is_cancelled() {
                        trace!("Stopping incoming messages manager (worker stopping).");
                        break;
                    }
                }
            }
        }

        let _ = frame_queue.join().await;

        Ok(())
    }
}
