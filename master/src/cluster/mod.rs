use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use atomic::Atomic;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use miette::Result;
use miette::{miette, Context, IntoDiagnostic};
use shared::cancellation::CancellationToken;
use shared::jobs::{BlenderJob, DistributionStrategy};
use shared::messages::handshake::{
    MasterHandshakeAcknowledgement,
    MasterHandshakeRequest,
    WorkerHandshakeResponse,
    WorkerHandshakeType,
    WorkerID,
};
use shared::messages::job::MasterJobStartedEvent;
use shared::messages::receive_exact_message_from_stream;
use shared::messages::traits::IntoWebSocketMessage;
use shared::results::master_trace::MasterTrace;
use shared::results::worker_trace::WorkerTrace;
use shared::websockets::DEFAULT_WEBSOCKET_CONFIG;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{accept_async_with_config, WebSocketStream};
use tracing::{debug, error, info, trace, warn};

use crate::cluster::state::ClusterManagerState;
use crate::cluster::strategies::{
    dynamic_distribution_strategy,
    naive_coarse_distribution_strategy,
    naive_fine_distribution_strategy,
};
use crate::connection::Worker;

pub mod state;
pub mod strategies;

#[derive(Eq, PartialEq, Hash, Debug, Copy, Clone)]
pub enum ClientConnectionStatus {
    Disconnected,
    Connected { address: SocketAddr },
}

impl ClientConnectionStatus {
    pub fn address(&self) -> Option<SocketAddr> {
        match self {
            ClientConnectionStatus::Disconnected => None,
            ClientConnectionStatus::Connected { address } => Some(*address),
        }
    }
}


pub struct ReconnectableClientConnection {
    pub worker_id: WorkerID,

    pub connection_status: Atomic<ClientConnectionStatus>,

    max_receive_delay: Duration,

    max_send_delay: Duration,

    websocket_sink: Arc<Mutex<SplitSink<WebSocketStream<TcpStream>, Message>>>,
    websocket_stream: Arc<Mutex<SplitStream<WebSocketStream<TcpStream>>>>,
}

impl ReconnectableClientConnection {
    pub fn new(
        worker_id: WorkerID,
        websocket_connection: WebSocketStream<TcpStream>,
        address: SocketAddr,
        max_receive_delay: Duration,
        max_send_delay: Duration,
    ) -> Self {
        let (ws_sink, ws_stream) = websocket_connection.split();

        let ws_sink_arc_mutex = Arc::new(Mutex::new(ws_sink));
        let ws_stream_arc_mutex = Arc::new(Mutex::new(ws_stream));

        Self {
            worker_id,
            connection_status: Atomic::new(ClientConnectionStatus::Connected { address }),
            max_receive_delay,
            max_send_delay,
            websocket_sink: ws_sink_arc_mutex,
            websocket_stream: ws_stream_arc_mutex,
        }
    }

    pub async fn replace_inner_connection(
        &self,
        websocket_connection: WebSocketStream<TcpStream>,
        new_address: SocketAddr,
    ) {
        self.connection_status.store(
            ClientConnectionStatus::Disconnected,
            Ordering::SeqCst,
        );

        let mut locked_sink = self.websocket_sink.lock().await;
        let mut locked_stream = self.websocket_stream.lock().await;


        let (ws_sink, ws_stream) = websocket_connection.split();

        *locked_sink = ws_sink;
        *locked_stream = ws_stream;


        self.connection_status.store(
            ClientConnectionStatus::Connected {
                address: new_address,
            },
            Ordering::SeqCst,
        );
    }

    pub async fn receive_message(&self) -> Result<Message> {
        let receive_request_begin_time = Instant::now();

        loop {
            if receive_request_begin_time.elapsed() > self.max_receive_delay {
                return Err(miette!(
                    "Failed to receive message after {:.3} seconds.",
                    receive_request_begin_time.elapsed().as_secs_f64()
                ));
            }

            {
                let connection_status = self.connection_status.load(Ordering::SeqCst);
                if connection_status == ClientConnectionStatus::Disconnected {
                    trace!(
                        worker_id = %self.worker_id,
                        "Currently reconnecting, waiting 50 ms before retrying receive from worker."
                    );

                    tokio::time::sleep(Duration::from_millis(50)).await;

                    continue;
                }
            }

            let mut locked_stream = self.websocket_stream.lock().await;
            return match locked_stream.next().await {
                Some(message) => match message {
                    Ok(message) => Ok(message),
                    Err(error) => {
                        error!(
                            worker_id = %self.worker_id,
                            error = ?error,
                            "Could not receive message from WebSocket stream, will wait for reconnect."
                        );

                        self.connection_status.store(
                            ClientConnectionStatus::Disconnected,
                            Ordering::SeqCst,
                        );

                        drop(locked_stream);

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

        // TODO Introduce maximum delay after which this will return Err!
        loop {
            if send_request_begin_time.elapsed() > self.max_send_delay {
                return Err(miette!(
                    "Failed to send message after {:.3} seconds.",
                    send_request_begin_time.elapsed().as_secs_f64()
                ));
            }


            {
                let connection_status = self.connection_status.load(Ordering::SeqCst);
                if connection_status == ClientConnectionStatus::Disconnected {
                    debug!("Currently reconnecting, waiting 50 ms before retrying send.");

                    tokio::time::sleep(Duration::from_millis(50)).await;

                    continue;
                }
            }

            let mut locked_sink = self.websocket_sink.lock().await;
            return match locked_sink.send(message.clone()).await {
                Ok(_) => Ok(()),
                Err(error) => {
                    error!(
                        worker_id = %self.worker_id,
                        error = ?error,
                        "Could not send message through WebSocket stream, will wait for reconnect."
                    );

                    self.connection_status.store(
                        ClientConnectionStatus::Disconnected,
                        Ordering::SeqCst,
                    );

                    drop(locked_sink);

                    continue;
                }
            };
        }
    }

    pub fn address(&self) -> Option<SocketAddr> {
        match self.connection_status.load(Ordering::SeqCst) {
            ClientConnectionStatus::Disconnected => None,
            ClientConnectionStatus::Connected { address } => Some(address),
        }
    }
}


pub struct ReconnectingWebSocketServer {
    pub connections: Arc<Mutex<HashMap<WorkerID, Arc<ReconnectableClientConnection>>>>,

    pub acceptor_join_handle: JoinHandle<Result<()>>,
}

impl ReconnectingWebSocketServer {
    pub async fn new(
        listener: TcpListener,
        cancellation_token: CancellationToken,
        cluster_state: Arc<ClusterManagerState>,
    ) -> Self {
        let worker_map = Arc::new(Mutex::new(HashMap::new()));

        let acceptor_join_handle = tokio::spawn(Self::indefinitely_accept_connections(
            listener,
            worker_map.clone(),
            cluster_state,
            cancellation_token,
        ));

        Self {
            connections: worker_map,
            acceptor_join_handle,
        }
    }

    async fn indefinitely_accept_connections(
        listener: TcpListener,
        connection_map: Arc<Mutex<HashMap<WorkerID, Arc<ReconnectableClientConnection>>>>,
        cluster_state: Arc<ClusterManagerState>,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        loop {
            if let Ok(new_tcp_connection) =
                tokio::time::timeout(Duration::from_secs(2), listener.accept()).await
            {
                let (tcp_stream, worker_address) = new_tcp_connection
                    .into_diagnostic()
                    .wrap_err_with(|| miette!("Could not accept incoming connection."))?;

                debug!(
                    address = %worker_address,
                    "Accepted new TCP connection, spawning task to perform handshake."
                );

                tokio::spawn(Self::initialize_worker_connection(
                    worker_address,
                    tcp_stream,
                    connection_map.clone(),
                    cluster_state.clone(),
                    cancellation_token.clone(),
                ));
            }

            if cancellation_token.is_cancelled() {
                info!("Cancellation token set, waiting for all worker connections to drop.");

                {
                    let mut locked_workers = cluster_state.workers.lock().await;
                    for (_, worker) in locked_workers.drain() {
                        if let Some(address) = worker.connection.address() {
                            debug!(
                                worker_id = %worker.id,
                                address = %address,
                                "Calling .join() on worker.",
                            );
                        } else {
                            warn!(
                                worker_id = %worker.id,
                                "Calling .join() on disconnected worker."
                            );
                        }
                    }
                }

                info!("Stopping worker connection accepting task.");
                break;
            }
        }

        Ok(())
    }

    async fn initialize_worker_connection(
        address: SocketAddr,
        tcp_stream: TcpStream,
        worker_map: Arc<Mutex<HashMap<WorkerID, Arc<ReconnectableClientConnection>>>>,
        cluster_state: Arc<ClusterManagerState>,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        // Perform the WebSocket protocol handshake.
        let mut ws_stream = accept_async_with_config(tcp_stream, Some(DEFAULT_WEBSOCKET_CONFIG))
            .await
            .into_diagnostic()
            .wrap_err_with(|| {
                miette!(
                    "Failed to accept TcpStream as WebSocket stream (protocol handshake failed)."
                )
            })?;

        info!(
            worker_address = %address,
            "New WebSocket connection accepted (protocol handshake OK)."
        );


        // Perform our internal handshake over the given WebSocket.
        // This handshake consists of three messages:
        //  1. The master server must send a "handshake request".
        //  2. The worker must respond with a "handshake response".
        //  3. The master server must confirm with a "handshake response acknowledgement".
        //
        // After these three steps, the connection is considered to be established.
        let handshake_request = MasterHandshakeRequest::new("1.0.0")
            .into_ws_message()
            .to_tungstenite_message()?;

        ws_stream
            .send(handshake_request)
            .await
            .into_diagnostic()
            .wrap_err_with(|| miette!("Failed to send handshake request."))?;

        info!(
            worker_address = %address,
            "Sent handshake request to worker."
        );


        let handshake_response: WorkerHandshakeResponse =
            receive_exact_message_from_stream(&mut ws_stream)
                .await
                .wrap_err_with(|| miette!("Failed to receive handshake response from worker."))?;

        info!(
            worker_address = %address,
            worker_id = %handshake_response.worker_id,
            handshake_type = %handshake_response.handshake_type,
            "Received handshake response from worker."
        );

        // If the worker is reconnecting, we must already have it in our worker map,
        // otherwise something went *really* wrong.
        let is_ok = if handshake_response.handshake_type == WorkerHandshakeType::Reconnecting {
            let locked_worker_map = worker_map.lock().await;

            locked_worker_map.contains_key(&handshake_response.worker_id)
        } else {
            true
        };


        let handshake_ack = MasterHandshakeAcknowledgement::new(is_ok)
            .into_ws_message()
            .to_tungstenite_message()?;

        ws_stream
            .send(handshake_ack)
            .await
            .into_diagnostic()
            .wrap_err_with(|| miette!("Failed to send handshake ack."))?;

        info!(
            worker_address = %address,
            worker_id = %handshake_response.worker_id,
            "Sent handshake acknowledgement to worker."
        );


        // Both the WebSocket and our internal handshake has completed at this point.
        // If the worker is reconnecting, we need to get our existing reconnectable connection and
        // replace its inner WebSocket stream, otherwise we simply instantiate a new one and put it
        // into the map.

        match handshake_response.handshake_type {
            WorkerHandshakeType::FirstConnection => {
                let connection_arc = {
                    let mut locked_connection_map = worker_map.lock().await;

                    let connection = ReconnectableClientConnection::new(
                        handshake_response.worker_id,
                        ws_stream,
                        address,
                        Duration::from_secs(30),
                        Duration::from_secs(30),
                    );
                    let connection_arc = Arc::new(connection);

                    locked_connection_map.insert(
                        handshake_response.worker_id,
                        connection_arc.clone(),
                    );

                    connection_arc
                };


                let heartbeat_cancellation_token = CancellationToken::new();
                let worker = Worker::initialize_from_handshaked_connection(
                    connection_arc,
                    cluster_state.clone(),
                    heartbeat_cancellation_token,
                    cancellation_token.clone(),
                )
                .await
                .wrap_err_with(|| miette!("Failed to set up worker."))?;

                {
                    let mut locked_worker_map = cluster_state.workers.lock().await;
                    locked_worker_map.insert(worker.id, worker);
                }

                info!(
                    worker_address = %address,
                    worker_id = %handshake_response.worker_id,
                    "Worker has successfully established an initial connection."
                );
            }
            WorkerHandshakeType::Reconnecting => {
                let locked_worker_map = worker_map.lock().await;

                let existing_connection = locked_worker_map
                    .get(&handshake_response.worker_id)
                    .ok_or_else(|| {
                        miette!(
                            "BUG: Existing connection doesn't exist, even though we just checked."
                        )
                    })?;

                existing_connection
                    .replace_inner_connection(ws_stream, address)
                    .await;

                // No need to update worker map, the Worker implementation
                // already uses an Arc-ed connection so the underlying connection
                // will be swapped out automatically.
                info!(
                    worker_address = %address,
                    worker_id = %handshake_response.worker_id,
                    "Worker has successfully reconnected."
                );
            }
        };

        Ok(())
    }
}


pub struct ClusterManager {}

impl ClusterManager {
    pub async fn initialize_server_and_run_job(
        host: &str,
        port: usize,
        job: BlenderJob,
    ) -> Result<(MasterTrace, Vec<(String, WorkerTrace)>)> {
        let shared_state = Arc::new(ClusterManagerState::new_from_job(job.clone()));
        let cancellation_token = CancellationToken::new();

        let server_socket = TcpListener::bind(format!("{host}:{port}"))
            .await
            .into_diagnostic()?;

        let reconnecting_server = ReconnectingWebSocketServer::new(
            server_socket,
            cancellation_token.clone(),
            shared_state.clone(),
        )
        .await;

        let master_trace = Self::wait_for_workers_and_run_job(job, shared_state.clone())
            .await
            .wrap_err_with(|| miette!("Failed to wait for workers and run job."))?;

        info!("Job finished, requesting performance traces from all workers.");

        // TODO Add tracing for reconnection events (on the worker side)
        let mut worker_traces: Vec<(String, WorkerTrace)> = Vec::new();
        {
            let locked_workers = shared_state.workers.lock().await;

            for worker in locked_workers.values() {
                // Prevents the per-worker heartbeat task from sending any more messages through the WebSocket.
                worker.heartbeat_cancellation_token.cancel();

                let worker_trace = worker
                    .requester
                    .finish_job_and_get_trace()
                    .await
                    .wrap_err_with(|| {
                        miette!(
                            "Could not receive trace from worker {:?}!",
                            worker.id
                        )
                    })?;

                let address_string = match worker.connection.address() {
                    Some(address) => address.to_string(),
                    None => String::from("unknown"),
                };

                worker_traces.push((
                    format!("{}-{}", worker.id, address_string,),
                    worker_trace,
                ));
            }
        }

        trace!("Setting global cancellation token to true.");
        cancellation_token.cancel();

        trace!("Waiting for worker connection acceptor to stop.");
        reconnecting_server
            .acceptor_join_handle
            .await
            .into_diagnostic()??;

        Ok((master_trace, worker_traces))
    }

    async fn wait_for_workers_and_run_job(
        job: BlenderJob,
        state: Arc<ClusterManagerState>,
    ) -> Result<MasterTrace> {
        /*
         * Wait for `job.wait_for_number_of_workers` workers to connect.
         */
        info!(
            "Waiting for at least {} workers to connect before starting job.",
            job.wait_for_number_of_workers
        );

        loop {
            let num_clients = {
                let workers_locked = &state.workers.lock().await;
                workers_locked.len()
            };

            if num_clients >= job.wait_for_number_of_workers {
                break;
            }

            trace!(
                "{}/{} workers currently connected.",
                num_clients,
                job.wait_for_number_of_workers
            );

            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        /*
         * Send job starting events to all connected workers.
         */
        let systime_job_start = SystemTime::now();

        {
            let workers_locked = &state.workers.lock().await;

            let workers_amount = workers_locked.len();
            info!(
                "READY! {} workers are connected, starting job!",
                workers_amount
            );

            for (_, worker) in workers_locked.iter() {
                let worker_sender_handle = worker.sender.sender_handle();

                worker_sender_handle
                    .send_message(MasterJobStartedEvent::new())
                    .await
                    .wrap_err_with(|| {
                        miette!(
                            "Could not send job started event to worker {:?}",
                            worker.id
                        )
                    })?;
            }
        }

        // FIXME If a client connects after a job has started, they will not receive the job start event.
        // FIXME Disallow initial handshakes after a job has started, only reconnections.

        /*
         * Run the Blender job to completion.
         */
        let distribution_result = match job.frame_distribution_strategy {
            DistributionStrategy::NaiveFine => {
                info!("Running job with strategy: naive fine.");

                naive_fine_distribution_strategy(&job, state.clone())
                    .await
                    .wrap_err_with(|| {
                        miette!("Failed to complete naive fine distribution strategy.")
                    })
            }
            DistributionStrategy::EagerNaiveCoarse { target_queue_size } => {
                info!(
                    "Running job with strategy: naive coarse (target_queue_size={})",
                    target_queue_size
                );

                naive_coarse_distribution_strategy(&job, state.clone(), target_queue_size)
                    .await
                    .wrap_err_with(|| {
                        miette!("Failed to complete naive coarse distribution strategy.")
                    })
            }
            DistributionStrategy::Dynamic(options) => {
                info!(
                    "Running job with strategy: dynamic (target_queue_size={})",
                    options.target_queue_size
                );

                dynamic_distribution_strategy(&job, state.clone(), options)
                    .await
                    .wrap_err_with(|| miette!("Failed to complete dynamic distribution strategy."))
            }
        };

        if let Err(error) = distribution_result {
            error!(
                "Errored while running frame distribution: {}",
                error
            );
            return Err(error);
        }

        info!("All frames have been finished!");

        let systime_job_finish = SystemTime::now();

        let performance = MasterTrace::new(systime_job_start, systime_job_finish);

        Ok(performance)
    }
}
