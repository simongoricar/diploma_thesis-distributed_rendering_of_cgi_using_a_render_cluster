pub mod event_dispatcher;
pub mod queue;

use std::io;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt, TryStreamExt};
use log::{debug, info};
use miette::{miette, Context, IntoDiagnostic, Result};
use shared::messages::handshake::{
    MasterHandshakeAcknowledgement,
    MasterHandshakeRequest,
    WorkerHandshakeResponse,
};
use shared::messages::heartbeat::MasterHeartbeatRequest;
use shared::messages::traits::IntoWebSocketMessage;
use shared::messages::{traits, WebSocketMessage};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{accept_async, tungstenite, WebSocketStream};

use crate::websockets::worker::event_dispatcher::WorkerEventDispatcher;
use crate::websockets::worker::queue::WorkerQueue;

pub enum WorkerConnectionState {
    PendingHandshake,
    Connected,
}

fn parse_websocket_message(message: Message) -> Result<Option<WebSocketMessage>> {
    match message {
        Message::Text(text_message) => Ok(Some(WebSocketMessage::from_json_string(
            text_message,
        )?)),
        _ => Ok(None),
    }
}

async fn receive_exact_message<M: traits::Message + TryFrom<WebSocketMessage>>(
    receiver_channel: &mut UnboundedReceiver<WebSocketMessage>,
) -> Result<M> {
    let next_message = receiver_channel
        .next()
        .await
        .ok_or_else(|| miette!("Could not get next incoming message."))?;

    if let Ok(message) = next_message.try_into() {
        Ok(message)
    } else {
        Err(miette!("Unexpected incoming message type."))
    }
}

pub struct Worker {
    pub address: SocketAddr,

    pub sender_channel: Arc<UnboundedSender<Message>>,

    pub connection_state: WorkerConnectionState,

    pub event_dispatcher: Arc<WorkerEventDispatcher>,

    pub queue: Arc<Mutex<WorkerQueue>>,

    pub connection_tasks: JoinSet<Result<()>>,
}

impl Worker {
    pub async fn new_with_accept_and_handshake(
        stream: TcpStream,
        address: SocketAddr,
    ) -> Result<Self> {
        let queue = Arc::new(Mutex::new(WorkerQueue::new()));

        let (message_sender_channel, event_dispatcher, connection_tasks) =
            Self::accept_ws_stream_and_initialize_tasks(stream, queue.clone()).await?;

        // TODO Integrate queue
        Ok(Self {
            address,
            sender_channel: message_sender_channel,
            connection_state: WorkerConnectionState::Connected,
            event_dispatcher,
            queue,
            connection_tasks,
        })
    }

    /*
     * Public methods
     */

    // TODO

    /*
     * Private WebSocket accepting, handshaking and other connection code.
     */

    async fn accept_ws_stream_and_initialize_tasks(
        stream: TcpStream,
        queue: Arc<Mutex<WorkerQueue>>,
    ) -> Result<(
        Arc<UnboundedSender<Message>>,
        Arc<WorkerEventDispatcher>,
        JoinSet<Result<()>>,
    )> {
        let address = stream.peer_addr().into_diagnostic()?;
        let ws_stream = accept_async(stream)
            .await
            .into_diagnostic()
            .wrap_err_with(|| miette!("Could not accept TcpStream."))?;

        let (ws_sink, ws_stream) = ws_stream.split();

        // To send messages through the WebSocket, send a Message instance through this unbounded channel.
        let (ws_sender_tx, ws_sender_rx) = futures_channel::mpsc::unbounded::<Message>();
        // To see received messages, read this channel.
        let (ws_receiver_tx, ws_receiver_rx) =
            futures_channel::mpsc::unbounded::<WebSocketMessage>();

        let sender_channel = Arc::new(ws_sender_tx);
        let receiver_channel = Arc::new(Mutex::new(ws_receiver_rx));


        let mut worker_connection_future_set: JoinSet<Result<()>> = JoinSet::new();

        let incoming_messages_handler =
            Self::forward_incoming_messages_through_channel(ws_stream, ws_receiver_tx);
        worker_connection_future_set.spawn(incoming_messages_handler);

        let outgoing_messages_handler =
            Self::forward_queued_outgoing_messages_through_websocket(ws_sink, ws_sender_rx);
        worker_connection_future_set.spawn(outgoing_messages_handler);

        let handshake_handle = worker_connection_future_set.spawn(Self::perform_handshake(
            address,
            sender_channel.clone(),
            receiver_channel.clone(),
        ));

        worker_connection_future_set.join_next().await;
        if !handshake_handle.is_finished() {
            return Err(miette!("BUG: Incorrect task completed."));
        }

        let event_dispatcher = WorkerEventDispatcher::new(
            Arc::try_unwrap(receiver_channel)
                .map_err(|_| {
                    miette!(
                        "BUG: failed to unwrap receiver channel Arc (is handshake still running?!)"
                    )
                })?
                .into_inner(),
        )
        .await;
        let event_dispatcher_arc = Arc::new(event_dispatcher);

        worker_connection_future_set.spawn(Self::maintain_heartbeat(
            event_dispatcher_arc.clone(),
            sender_channel.clone(),
        ));

        worker_connection_future_set.spawn(Self::manage_incoming_messages(
            event_dispatcher_arc.clone(),
            queue,
        ));

        Ok((
            sender_channel,
            event_dispatcher_arc,
            worker_connection_future_set,
        ))
    }

    async fn perform_handshake(
        worker_address: SocketAddr,
        sender_channel: Arc<UnboundedSender<Message>>,
        receiver_channel: Arc<Mutex<UnboundedReceiver<WebSocketMessage>>>,
    ) -> Result<()> {
        info!("[{worker_address:?}] Sending handshake request.");

        MasterHandshakeRequest::new("1.0.0")
            .into_ws_message()
            .send(&sender_channel)
            .wrap_err_with(|| miette!("Could not send initial handshake request."))?;

        let response = {
            let mut locked_receiver = receiver_channel.lock().await;
            receive_exact_message::<WorkerHandshakeResponse>(&mut locked_receiver)
                .await
                .wrap_err_with(|| miette!("Invalid message: expected worker handshake response!"))?
        };

        info!(
            "[{worker_address:?}] Got handshake response from worker: worker_version={}. Sending acknowledgement.",
            response.worker_version
        );

        MasterHandshakeAcknowledgement::new(true)
            .into_ws_message()
            .send(&sender_channel)
            .wrap_err_with(|| miette!("Could not send handshake acknowledgement."))?;

        info!("[{worker_address:?}] Handshake has been completed.");

        Ok(())
    }

    async fn forward_incoming_messages_through_channel(
        stream: SplitStream<WebSocketStream<TcpStream>>,
        message_sender_channel: UnboundedSender<WebSocketMessage>,
    ) -> Result<()> {
        stream
            .try_for_each(|ws_message| async {
                let message = match parse_websocket_message(ws_message) {
                    Ok(optional_message) => match optional_message {
                        Some(concrete_message) => concrete_message,
                        None => {
                            return Ok(());
                        }
                    },
                    Err(error) => {
                        return Err(tungstenite::Error::Io(io::Error::new(
                            ErrorKind::Other,
                            error.to_string(),
                        )));
                    }
                };

                let channel_send_result = message_sender_channel.unbounded_send(message);
                if let Err(error) = channel_send_result {
                    return Err(tungstenite::Error::Io(io::Error::new(
                        ErrorKind::Other,
                        error.to_string(),
                    )));
                }

                Ok(())
            })
            .await
            .into_diagnostic()
            .wrap_err_with(|| miette!("Failed to receive incoming WebSocket message."))
    }

    async fn forward_queued_outgoing_messages_through_websocket(
        mut ws_sink: SplitSink<WebSocketStream<TcpStream>, Message>,
        mut message_send_queue_receiver: UnboundedReceiver<Message>,
    ) -> Result<()> {
        loop {
            let next_outgoing_message = message_send_queue_receiver
                .next()
                .await
                .ok_or_else(|| miette!("Can't get outgoing message from channel!"))?;

            ws_sink
                .send(next_outgoing_message)
                .await
                .into_diagnostic()
                .wrap_err_with(|| miette!("Could not send outgoing message, sink failed."))?;
        }
    }

    async fn manage_incoming_messages(
        event_dispatcher: Arc<WorkerEventDispatcher>,
        queue: Arc<Mutex<WorkerQueue>>,
    ) -> Result<()> {
        info!("Running incoming messages parsing loop");

        // TODO parse incoming queue item finished messages, etc.
        //      When a queue item is finished, update the internal `queue`.

        Ok(())
    }

    async fn maintain_heartbeat(
        event_dispatcher: Arc<WorkerEventDispatcher>,
        sender_channel: Arc<UnboundedSender<Message>>,
    ) -> Result<()> {
        info!("Running heartbeat loop.");

        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;

            debug!("Sending heartbeat request to worker.");
            MasterHeartbeatRequest::new()
                .into_ws_message()
                .send(&sender_channel)
                .wrap_err_with(|| miette!("Could not send heartbeat request."))?;


            let time_heartbeat_wait_start = Instant::now();

            event_dispatcher
                .wait_for_heartbeat_response(Duration::from_secs(5))
                .await
                .wrap_err_with(|| miette!("Worker did not respond to heartbeat."))?;

            let time_heartbeat_wait_latency = time_heartbeat_wait_start.elapsed();
            debug!(
                "Worker responded to heartbeat in {:.4} seconds.",
                time_heartbeat_wait_latency.as_secs_f64()
            );
        }
    }
}
