use std::collections::HashMap;
use std::io;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::Arc;

use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures_util::future::BoxFuture;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt, TryStreamExt};
use log::{info, warn};
use miette::{miette, Context, IntoDiagnostic, Result};
use shared::messages::handshake::{
    MasterHandshakeAcknowledgement,
    MasterHandshakeRequest,
    WorkerHandshakeResponse,
};
use shared::messages::heartbeat::WorkerHeartbeatResponse;
use shared::messages::queue::WorkerFrameQueueItemFinishedNotification;
use shared::messages::traits::IntoWebSocketMessage;
use shared::messages::{traits, WebSocketMessage};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{accept_async, tungstenite, WebSocketStream};

pub enum WorkerConnectionState {
    PendingHandshake,
    Connected {
        sender: Arc<UnboundedSender<Message>>,
        receiver: Arc<Mutex<UnboundedReceiver<WebSocketMessage>>>,
    },
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

#[derive(Eq, PartialEq, Hash)]
pub enum WorkerEventType {
    HeartbeatResponse,
    QueueItemFinishedNotification,
    // TODO
}

#[derive(Eq, PartialEq, Hash, Copy, Clone)]
pub struct EventSubscriberHandle(usize);
impl EventSubscriberHandle {
    pub fn generate_random() -> Self {
        Self(rand::random::<usize>())
    }
}

pub type HeartbeatResponseBoxedAsyncCallback =
    Box<dyn (Fn(WorkerHeartbeatResponse) -> BoxFuture<'static, ()>) + Send>;
pub type HeartbeatResponseSubscribers =
    HashMap<EventSubscriberHandle, HeartbeatResponseBoxedAsyncCallback>;

pub type QueueItemFinishedBoxedAsyncCallback =
    Box<dyn (Fn(WorkerFrameQueueItemFinishedNotification) -> BoxFuture<'static, ()>) + Send>;
pub type QueueItemFinishedSubscribers =
    HashMap<EventSubscriberHandle, QueueItemFinishedBoxedAsyncCallback>;

pub struct WorkerEventDispatcher {
    sender_channel: Arc<UnboundedSender<Message>>,

    subscribers_heartbeat_response: Arc<Mutex<HeartbeatResponseSubscribers>>,
    subscribers_queue_item_finished: Arc<Mutex<QueueItemFinishedSubscribers>>,
}

impl WorkerEventDispatcher {
    pub async fn new(
        sender_channel: Arc<UnboundedSender<Message>>,
        receiver_channel: UnboundedReceiver<WebSocketMessage>,
    ) -> Self {
        let new_self = Self {
            sender_channel,
            subscribers_heartbeat_response: Arc::new(Mutex::new(HashMap::new())),
            subscribers_queue_item_finished: Arc::new(Mutex::new(HashMap::new())),
        };

        tokio::spawn(Self::run(
            new_self.subscribers_heartbeat_response.clone(),
            new_self.subscribers_queue_item_finished.clone(),
            receiver_channel,
        ));

        new_self
    }

    async fn run(
        subscribers_heartbeat_response: Arc<Mutex<HeartbeatResponseSubscribers>>,
        subscribers_queue_item_finished: Arc<Mutex<QueueItemFinishedSubscribers>>,
        mut receiver_channel: UnboundedReceiver<WebSocketMessage>,
    ) {
        info!("WorkerEventDispatcher: Running event distribution loop.");

        loop {
            let next_message = match receiver_channel.next().await {
                Some(message) => message,
                None => {
                    warn!("WorkerEventDispatcher: Can't receive message from channel, aborting event dispatcher task.");
                    return;
                }
            };

            match next_message {
                WebSocketMessage::WorkerFrameQueueItemFinishedNotification(notification) => {
                    Self::emit_queue_item_finished(&subscribers_queue_item_finished, notification)
                        .await;
                }
                WebSocketMessage::WorkerHeartbeatResponse(response) => {
                    Self::emit_heartbeat_response(&subscribers_heartbeat_response, response).await;
                }
                _ => {
                    warn!(
                        "WorkerEventDispatcher: Unexpected (but valid) incoming WebSocket message: {}",
                        next_message.type_name()
                    );
                }
            }
        }
    }

    /*
     * Private emitting methods
     */

    async fn emit_queue_item_finished(
        subscribers_queue_item_finished: &Arc<Mutex<QueueItemFinishedSubscribers>>,
        notification: WorkerFrameQueueItemFinishedNotification,
    ) {
        for callback in subscribers_queue_item_finished.lock().await.values() {
            tokio::spawn(callback(notification.clone()));
        }
    }

    async fn emit_heartbeat_response(
        subscribers_heartbeat_response: &Arc<Mutex<HeartbeatResponseSubscribers>>,
        response: WorkerHeartbeatResponse,
    ) {
        for callback in subscribers_heartbeat_response.lock().await.values() {
            tokio::spawn(callback(response.clone()));
        }
    }

    /*
     * Public event subscription and unsubscription methods.
     */

    pub async fn subscribe_to_queue_item_finished<C: Into<QueueItemFinishedBoxedAsyncCallback>>(
        &mut self,
        async_callback: C,
    ) -> EventSubscriberHandle {
        let mut locked_subscribers = self.subscribers_queue_item_finished.lock().await;

        let handle = EventSubscriberHandle::generate_random();
        if locked_subscribers.contains_key(&handle) {
            panic!("Incredibly rare random usize collision.");
        }

        locked_subscribers.insert(handle, async_callback.into());

        handle
    }

    pub async fn unsubscribe_from_queue_item_finished(
        &mut self,
        handle: EventSubscriberHandle,
    ) -> Result<()> {
        let mut locked_subscribers = self.subscribers_queue_item_finished.lock().await;

        if !locked_subscribers.contains_key(&handle) {
            return Err(miette!(
                "Invalid handle: subscriber doesn't exist!"
            ));
        }

        locked_subscribers.remove(&handle);

        Ok(())
    }

    pub async fn subscribe_to_heartbeat_response<C: Into<HeartbeatResponseBoxedAsyncCallback>>(
        &mut self,
        async_callback: C,
    ) -> EventSubscriberHandle {
        let mut locked_subscribers = self.subscribers_heartbeat_response.lock().await;

        let handle = EventSubscriberHandle::generate_random();
        if locked_subscribers.contains_key(&handle) {
            panic!("Incredibly rare random usize collision.");
        }

        locked_subscribers.insert(handle, async_callback.into());

        handle
    }

    pub async fn unsubscribe_from_heartbeat_response(
        &mut self,
        handle: EventSubscriberHandle,
    ) -> Result<()> {
        let mut locked_subscribers = self.subscribers_heartbeat_response.lock().await;

        if !locked_subscribers.contains_key(&handle) {
            return Err(miette!(
                "Invalid handle: subscriber doesn't exist!"
            ));
        }

        locked_subscribers.remove(&handle);

        Ok(())
    }

    // TODO Subscribing methods, async awaitable requests and such
}



pub struct Worker {
    pub address: SocketAddr,

    pub connection_state: WorkerConnectionState,

    pub event_dispatcher: WorkerEventDispatcher,
}

impl Worker {
    pub async fn new_with_accept_and_handshake(
        stream: TcpStream,
        address: SocketAddr,
    ) -> Result<Self> {
        let (sender_channel, receiver_channel) =
            Self::accept_ws_stream_and_perform_handshake(stream).await?;

        let event_dispatcher = WorkerEventDispatcher::new(
            sender_channel,
            Arc::try_unwrap(receiver_channel)
                .map_err(|_| {
                    miette!(
                        "BUG: failed to unwrap receiver channel Arc (is handshake still running?!)"
                    )
                })?
                .into_inner(),
        )
        .await;

        Ok(Self {
            address,
            connection_state: WorkerConnectionState::PendingHandshake,
            event_dispatcher,
        })
    }

    async fn accept_ws_stream_and_perform_handshake(
        stream: TcpStream,
    ) -> Result<(
        Arc<UnboundedSender<Message>>,
        Arc<Mutex<UnboundedReceiver<WebSocketMessage>>>,
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

        // TODO

        Ok((sender_channel, receiver_channel))
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
}
