pub mod receiver;
pub mod sender;

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use atomic::Atomic;
use chrono::Utc;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use miette::{miette, Context, IntoDiagnostic, Result};
use shared::cancellation::CancellationToken;
use shared::messages::handshake::{
    MasterHandshakeAcknowledgement,
    MasterHandshakeRequest,
    WorkerHandshakeResponse,
    WorkerID,
};
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
use shared::messages::traits::IntoWebSocketMessage;
use shared::messages::{receive_exact_message_from_stream, SenderHandle};
use shared::results::worker_trace::WorkerTraceBuilder;
use shared::websockets::DEFAULT_WEBSOCKET_CONFIG;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{client_async_with_config, WebSocketStream};
use tracing::{debug, error, info, trace, warn};

use crate::connection::receiver::MasterReceiver;
use crate::connection::sender::MasterSender;
use crate::rendering::queue::WorkerAutomaticQueue;
use crate::rendering::runner::BlenderJobRunner;

const TRACE_EVERY_NTH_PING: usize = 8;

#[derive(Eq, PartialEq, Hash, Copy, Clone)]
pub enum WebSocketConnectionStatus {
    Reconnecting,
    Connected,
}


pub struct ReconnectingWebSocketClient {
    pub worker_id: WorkerID,

    server_url: String,

    backoff_max_retries: usize,

    backoff_exp_base: f64,

    backoff_max_seconds: f64,

    max_receive_delay: Duration,

    max_send_delay: Duration,

    max_reconnection_retries: usize,

    cancellation_token: CancellationToken,

    pub connection_status: Atomic<WebSocketConnectionStatus>,

    pub websocket_sink: Arc<tokio::sync::Mutex<SplitSink<WebSocketStream<TcpStream>, Message>>>,

    pub websocket_stream: Arc<tokio::sync::Mutex<SplitStream<WebSocketStream<TcpStream>>>>,
}

impl ReconnectingWebSocketClient {
    #[allow(clippy::too_many_arguments)]
    pub async fn new_connection<S: Into<String>>(
        server_url: S,
        backoff_max_retries: usize,
        backoff_exp_base: f64,
        backoff_max_seconds: f64,
        max_receive_delay: Duration,
        max_send_delay: Duration,
        max_reconnection_retries: usize,
        cancellation_token: CancellationToken,
    ) -> Result<Self> {
        let server_url = server_url.into();
        let worker_id = WorkerID::generate();

        let web_socket_connection = Self::connect_to_master_server_and_handshake(
            &server_url,
            backoff_max_retries,
            backoff_exp_base,
            backoff_max_seconds,
            worker_id,
            false,
        )
        .await?;

        // We have concluded the setup at this point and need to update the connection status, then
        // simply set the sink and stream on this instance.
        let (ws_sink, ws_stream) = web_socket_connection.split();

        let ws_sink_arc_mutex = Arc::new(tokio::sync::Mutex::new(ws_sink));
        let ws_stream_arc_mutex = Arc::new(tokio::sync::Mutex::new(ws_stream));

        Ok(Self {
            worker_id,
            server_url,
            backoff_max_retries,
            backoff_exp_base,
            backoff_max_seconds,
            max_receive_delay,
            max_send_delay,
            max_reconnection_retries,
            cancellation_token,
            connection_status: Atomic::new(WebSocketConnectionStatus::Connected),
            websocket_sink: ws_sink_arc_mutex,
            websocket_stream: ws_stream_arc_mutex,
        })
    }

    pub async fn receive_message(&self) -> Result<Message> {
        let receive_request_begin_time = Instant::now();
        let mut reconnection_retries: usize = 0;

        loop {
            if self.cancellation_token.is_cancelled() {
                return Err(miette!(
                    "Cancellation token has been set, not receiving."
                ));
            } else if reconnection_retries > self.max_reconnection_retries {
                return Err(miette!(
                    "Failed to receive message after {reconnection_retries} reconnection retries."
                ));
            } else if receive_request_begin_time.elapsed() > self.max_receive_delay {
                return Err(miette!(
                    "Failed to receive message after {:.3} seconds.",
                    receive_request_begin_time.elapsed().as_secs_f64()
                ));
            }


            {
                let connection_status = self.connection_status.load(Ordering::SeqCst);

                if connection_status == WebSocketConnectionStatus::Reconnecting {
                    debug!("Currently reconnecting, waiting 50 ms before retrying receive.");

                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            }


            let mut locked_stream = self.websocket_stream.lock().await;
            return match locked_stream.next().await {
                Some(message) => match message {
                    Ok(message) => {
                        if reconnection_retries > 0 {
                            debug!("Message was successfully received after a reconnect.");
                        }

                        Ok(message)
                    }
                    Err(error) => {
                        if !self.cancellation_token.is_cancelled() {
                            error!(error = ?error, "Could not receive message from WebSocket stream, will attempt to reconnect.");

                            drop(locked_stream);

                            let connection_status = self.connection_status.load(Ordering::SeqCst);
                            if connection_status != WebSocketConnectionStatus::Reconnecting {
                                self.reconnect().await.wrap_err_with(|| {
                                    miette!("Could not reconnect to master server.")
                                })?;
                            }

                            reconnection_retries += 1;
                        }

                        continue;
                    }
                },
                None => Err(miette!(
                    "No next message in the WebSocket stream."
                )),
            };
        }
    }

    pub async fn send_message(&self, message: Message) -> Result<()> {
        let send_request_begin_time = Instant::now();
        let mut reconnection_retries: usize = 0;

        loop {
            if self.cancellation_token.is_cancelled() {
                return Err(miette!(
                    "Cancellation token has been set, not sending."
                ));
            } else if reconnection_retries > self.max_reconnection_retries {
                return Err(miette!(
                    "Failed to send message after {reconnection_retries} reconnection retries."
                ));
            } else if send_request_begin_time.elapsed() > self.max_send_delay {
                return Err(miette!(
                    "Failed to send message after {:.3} seconds.",
                    send_request_begin_time.elapsed().as_secs_f64()
                ));
            }


            {
                let connection_status = self.connection_status.load(Ordering::SeqCst);

                if connection_status == WebSocketConnectionStatus::Reconnecting {
                    debug!("Currently reconnecting, waiting 50 ms before retrying send.");

                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            }

            let mut locked_sink = self.websocket_sink.lock().await;
            return match locked_sink.send(message.clone()).await {
                Ok(_) => Ok(()),
                Err(error) => {
                    if !self.cancellation_token.is_cancelled() {
                        error!(
                            error = ?error,
                            "Could not send message through WebSocket stream, will attempt to reconnect."
                        );

                        drop(locked_sink);

                        let connection_status = self.connection_status.load(Ordering::SeqCst);
                        if connection_status != WebSocketConnectionStatus::Reconnecting {
                            self.reconnect().await.wrap_err_with(|| {
                                miette!("Could not reconnect to master server.")
                            })?;
                        }

                        reconnection_retries += 1;
                    }

                    continue;
                }
            };
        }
    }

    /*
     * Private methods
     */

    async fn connect_to_master_server_and_handshake(
        server_url: &str,
        max_retries: usize,
        exp_base: f64,
        backoff_max_seconds: f64,
        worker_id: WorkerID,
        is_a_reconnect: bool,
    ) -> Result<WebSocketStream<TcpStream>> {
        // Establish TCP connection using exponential backoff if the connection cannot be made at the moment.
        let tcp_stream = Self::establish_tcp_connection_with_exponential_backoff(
            server_url,
            max_retries,
            exp_base,
            backoff_max_seconds,
        )
        .await
        .wrap_err_with(|| {
            miette!("Failed to establish TCP connection with master server at {server_url}")
        })?;


        // Perform both the WebSocket handshake as well as our internal handshake.
        let (mut web_socket, _) = client_async_with_config(
            format!("ws://{server_url}"),
            tcp_stream,
            Some(DEFAULT_WEBSOCKET_CONFIG),
        )
        .await
        .into_diagnostic()
        .wrap_err_with(|| {
            miette!("Failed to perform WebSocket handshake on given TCP connection.")
        })?;

        Self::perform_handshake(&mut web_socket, worker_id, is_a_reconnect).await?;

        Ok(web_socket)
    }

    async fn reconnect(&self) -> Result<()> {
        if self.cancellation_token.is_cancelled() {
            return Err(miette!(
                "Cancellation token has been set, not reconnecting."
            ));
        }

        self.connection_status.store(
            WebSocketConnectionStatus::Reconnecting,
            Ordering::SeqCst,
        );

        let mut locked_sink = self.websocket_sink.lock().await;
        let mut locked_stream = self.websocket_stream.lock().await;


        let web_socket_connection = Self::connect_to_master_server_and_handshake(
            &self.server_url,
            self.backoff_max_retries,
            self.backoff_exp_base,
            self.backoff_max_seconds,
            self.worker_id,
            true,
        )
        .await?;


        // We have concluded the setup at this point and need to update the connection status, then
        // simply replace the sink and stream on this instance.
        let (ws_sink, ws_stream) = web_socket_connection.split();

        *locked_sink = ws_sink;
        *locked_stream = ws_stream;

        self.connection_status.store(
            WebSocketConnectionStatus::Connected,
            Ordering::SeqCst,
        );

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
                server_url,
                "Trying to establish connection with master server.",
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
                retry = retries,
                next_retry_in = retry_in.as_secs_f64(),
                "Could not establish TCP connection with master server.",
            );
            retries += 1;

            tokio::time::sleep(retry_in).await;
        }

        Err(miette!(
            "Failed to establish connection with master server after {} retries.",
            retries
        ))
    }

    /// Performs our internal WebSocket handshake
    /// (master handshake request, worker response, master acknowledgement).
    async fn perform_handshake(
        websocket_stream: &mut WebSocketStream<TcpStream>,
        worker_id: WorkerID,
        is_a_reconnect: bool,
    ) -> Result<()> {
        info!("Waiting for handshake request from master server.");

        let handshake_request: MasterHandshakeRequest =
            receive_exact_message_from_stream(websocket_stream).await?;

        info!(
            server_version = handshake_request.server_version,
            "Got handshake request from master server."
        );


        let response_message = if !is_a_reconnect {
            WorkerHandshakeResponse::new_first_connection("1.0.0", worker_id)
                .into_ws_message()
                .to_tungstenite_message()?
        } else {
            WorkerHandshakeResponse::new_reconnection("1.0.0", worker_id)
                .into_ws_message()
                .to_tungstenite_message()?
        };

        websocket_stream
            .send(response_message)
            .await
            .into_diagnostic()
            .wrap_err_with(|| miette!("Failed to send handshake response."))?;

        info!(worker_id = ?worker_id, is_reconnect = is_a_reconnect, "Sent handshake response, waiting for acknowledgement.");


        let handshake_ack: MasterHandshakeAcknowledgement =
            receive_exact_message_from_stream(websocket_stream).await?;

        if !handshake_ack.ok {
            return Err(miette!(
                "Master server rejected us (acknowledgement not ok)."
            ));
        }


        if !is_a_reconnect {
            info!("Handshake finished - fully connected.");
        } else {
            info!("Handshake finished - fully reconnected.");
        }

        Ok(())
    }
}


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
        let global_cancellation_token = CancellationToken::new();

        let reconnecting_ws_client = ReconnectingWebSocketClient::new_connection(
            master_server_url,
            12,
            2f64,
            30f64,
            Duration::from_secs(30),
            Duration::from_secs(30),
            2,
            global_cancellation_token.clone(),
        )
        .await
        .wrap_err_with(|| miette!("Failed to initialize ReconnectingWebSocketClient."))?;
        let reconnecting_ws_client_arc = Arc::new(reconnecting_ws_client);


        let sender = MasterSender::new(
            reconnecting_ws_client_arc.clone(),
            global_cancellation_token.clone(),
        );

        let receiver = MasterReceiver::new(
            reconnecting_ws_client_arc.clone(),
            global_cancellation_token.clone(),
        );
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

        info!("Connection fully established and async tasks spawned.");

        let _ = manager_task_handle.await.into_diagnostic()?;
        let _ = heartbeat_task_handle.await.into_diagnostic()?;

        let _ = sender.join().await;

        let receiver = Arc::try_unwrap(receiver_arc)
            .expect("BUG: Something is holding a strong reference to the receiver!");
        let _ = receiver.join().await;

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
        let mut pings_since_traced: usize = 0;

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

            pings_since_traced += 1;
            if pings_since_traced >= TRACE_EVERY_NTH_PING {
                tracer
                    .trace_new_ping(
                        heartbeat_request.request_time,
                        request_received_time,
                    )
                    .await;

                pings_since_traced = 0;
            }

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
                    tracer.set_job_start_time(Utc::now()).await;
                },
                job_finished_request = job_finished_request_receiver.recv() => {
                    let request: MasterJobFinishedRequest = job_finished_request.into_diagnostic()?;

                    info!("Master server signalled job finish - sending trace and stopping.");
                    tracer.set_job_finish_time(Utc::now()).await;

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
