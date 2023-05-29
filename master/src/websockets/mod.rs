use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::{future, io};

use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{StreamExt, TryStreamExt};
use miette::{miette, IntoDiagnostic, Result};
use shared::jobs::BlenderJob;
use shared::messages::WebSocketMessage;
use tokio::net::TcpStream;
use tokio_tungstenite::{tungstenite, WebSocketStream};

pub enum ConnectionState {
    PerformingHandshake,
    Connected {
        sender: Arc<UnboundedSender<tungstenite::Message>>,
        receiver: Arc<tokio::sync::Mutex<UnboundedReceiver<WebSocketMessage>>>,
    },
}

pub struct ClientQueueItem {
    pub job: BlenderJob,
    pub frame_index: usize,
}

impl ClientQueueItem {
    pub fn new(job: BlenderJob, frame_index: usize) -> Self {
        Self { job, frame_index }
    }
}

pub struct ClientState {
    pub connection: ConnectionState,
    pub queue: Vec<ClientQueueItem>,
}

impl ClientState {
    pub fn new() -> Self {
        Self {
            connection: ConnectionState::PerformingHandshake,
            queue: Vec::new(),
        }
    }
}

pub struct Client {
    pub address: SocketAddr,
    pub state: ClientState,
}

impl Client {
    pub fn new(address: SocketAddr) -> Self {
        Self {
            address,
            state: ClientState::new(),
        }
    }

    pub fn set_connection_state(&mut self, state: ConnectionState) {
        self.state.connection = state;
    }

    pub fn sender(&self) -> Result<Arc<UnboundedSender<tungstenite::Message>>> {
        match &self.state.connection {
            ConnectionState::PerformingHandshake => Err(miette!(
                "Connection has not been fully established yet."
            )),
            ConnectionState::Connected { sender, .. } => Ok(sender.clone()),
        }
    }

    pub fn receiver(
        &self,
    ) -> Result<Arc<tokio::sync::Mutex<UnboundedReceiver<WebSocketMessage>>>>
    {
        match &self.state.connection {
            ConnectionState::PerformingHandshake => Err(miette!(
                "Connection has not been fully established yet."
            )),
            ConnectionState::Connected { receiver, .. } => Ok(receiver.clone()),
        }
    }

    pub fn add_to_queue(&mut self, job: BlenderJob, frame_index: usize) {
        self.state
            .queue
            .push(ClientQueueItem::new(job, frame_index));
    }

    pub fn remove_from_queue(&mut self, job_name: String, frame_index: usize) {
        self.state.queue.retain(|item| {
            item.job.job_name != job_name || item.frame_index != frame_index
        });
    }
}



pub async fn handle_incoming_client_messages(
    stream: SplitStream<WebSocketStream<TcpStream>>,
    receiver_queue: UnboundedSender<WebSocketMessage>,
) -> Result<()> {
    stream
        .try_for_each(|message| {
            match message {
                tungstenite::Message::Text(_) => {
                    let message = match WebSocketMessage::from_websocket_message(message) {
                        Ok(message) => message,
                        Err(_) => {
                            return future::ready(Err(tungstenite::Error::Io(io::Error::new(ErrorKind::Other, "Could not parse incoming Text message."))));
                        }
                    };

                    match receiver_queue.unbounded_send(message) {
                        Err(_) => {
                            future::ready(Err(tungstenite::Error::Io(io::Error::new(ErrorKind::Other, "Could not send received message over unbounded channel."))))
                        }
                        Ok(_) => {
                            future::ready(Ok(()))
                        }
                    }
                }
                tungstenite::Message::Binary(_) => {
                    todo!("Not implemented, need to handle binary websocket messages.");
                }
                _ => {
                    future::ready(Ok(()))
                }
            }
        })
        .await
        .into_diagnostic()?;

    Ok(())
}

pub async fn handle_outgoing_server_messages(
    ws_writer: SplitSink<WebSocketStream<TcpStream>, tungstenite::Message>,
    message_sender_channel_rx: UnboundedReceiver<tungstenite::Message>,
) -> Result<()> {
    message_sender_channel_rx
        .map(Ok)
        .forward(ws_writer)
        .await
        .into_diagnostic()?;

    Ok(())
}
