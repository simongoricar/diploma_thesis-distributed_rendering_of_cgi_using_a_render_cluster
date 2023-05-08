use std::collections::HashMap;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::{future, io};

use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures_util::future::try_join3;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{pin_mut, StreamExt, TryStreamExt};
use log::{debug, info};
use miette::{miette, Context, IntoDiagnostic, Result};
use shared::messages::handshake::{
    MasterHandshakeAcknowledgement,
    MasterHandshakeRequest,
};
use shared::messages::WebSocketMessage;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, tungstenite, WebSocketStream};

enum ClientState {
    PerformingHandshake,
    Connected,
}

struct Client {
    pub address: SocketAddr,
    pub state: ClientState,
}

impl Client {
    pub fn new(address: SocketAddr) -> Self {
        Self {
            address,
            state: ClientState::PerformingHandshake,
        }
    }

    pub fn set_state(&mut self, state: ClientState) {
        self.state = state;
    }
}

type ClientMap = Arc<Mutex<HashMap<SocketAddr, Client>>>;

async fn run_client_connection(
    address: SocketAddr,
    message_sender: UnboundedSender<tungstenite::Message>,
    mut message_receiver: UnboundedReceiver<WebSocketMessage>,
) -> Result<()> {
    info!("[{address:?}] Sending handshake request.");

    let handshake_request: WebSocketMessage =
        MasterHandshakeRequest::new("1.0.0").into();
    handshake_request.send(&message_sender).wrap_err_with(|| {
        miette!("Could not send initial handshake request.")
    })?;

    let handshake_response = {
        let next_message = message_receiver
            .next()
            .await
            .ok_or_else(|| miette!("No handshake response received."))?;

        match next_message {
            WebSocketMessage::WorkerHandshakeResponse(response) => response,
            _ => {
                return Err(miette!(
                    "Invalid handshake response: not handshake_response!"
                ));
            }
        }
    };

    info!(
        "[{address:?}] Got handshake response: worker_version={}",
        handshake_response.worker_version
    );

    let handshake_ack: WebSocketMessage =
        MasterHandshakeAcknowledgement::new(true).into();
    handshake_ack.send(&message_sender).wrap_err_with(|| {
        miette!("Could not send handshake acknowledgement.")
    })?;

    // TODO

    Ok(())
}

async fn handle_incoming_client_messages(
    stream: SplitStream<WebSocketStream<TcpStream>>,
    receiver_queue: UnboundedSender<WebSocketMessage>,
) -> Result<()> {
    // TODO Migrate to JSON, FlatBuffers are pointless at this stage.
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
                    todo!("Not implement, need to handle binary websocket messages.");
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

async fn handle_outgoing_server_messages(
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

async fn accept_and_set_up_client(
    client_map: ClientMap,
    tcp_stream: TcpStream,
    address: SocketAddr,
) -> Result<()> {
    debug!("[{address:?}] Accepting WebSocket connection.");
    let websocket_stream = accept_async(tcp_stream).await.into_diagnostic()?;
    info!("[{address:?}] Client accepted.");

    // Put the just-accepted client into the client map.
    {
        let mut map = client_map
            .lock()
            .expect("Client map lock has been poisoned.");

        map.insert(address, Client::new(address));
    }

    // Split the WebSocket stream into two parts: a sink for writing (sending) messages
    // and a stream for reading (receiving) messages.
    let (ws_write, ws_read) = websocket_stream.split();

    // To send messages through the WebSocket, send a Message instance through this unbounded channel.
    let (ws_sender_tx, ws_sender_rx) =
        futures_channel::mpsc::unbounded::<tungstenite::Message>();
    // To see received messages, read this channel.
    let (ws_receiver_tx, ws_receiver_rx) =
        futures_channel::mpsc::unbounded::<WebSocketMessage>();


    // Prepare sending and receiving futures.
    let send_queued_messages =
        handle_outgoing_server_messages(ws_write, ws_sender_rx);
    let receive_and_handle_messages =
        handle_incoming_client_messages(ws_read, ws_receiver_tx);

    // Spawn main handler.
    let client_connection_handler =
        run_client_connection(address, ws_sender_tx, ws_receiver_rx);

    // Pin and run both futures until the connection is dropped.
    pin_mut!(
        send_queued_messages,
        receive_and_handle_messages,
        client_connection_handler
    );

    info!("[{address:?}] Running main connection handler.");
    try_join3(
        send_queued_messages,
        receive_and_handle_messages,
        client_connection_handler,
    )
    .await?;

    // Remove the lost client from the client map.
    {
        let mut map = client_map
            .lock()
            .expect("Client map lock has been poisoned.");

        map.remove(&address);
    }

    info!("Client {:?} disconnected.", address);
    Ok(())
}


#[tokio::main]
async fn main() -> Result<()> {
    env_logger::try_init().into_diagnostic()?;

    let server_socket =
        TcpListener::bind("0.0.0.0:9901").await.into_diagnostic()?;
    info!("Server running at 0.0.0.0:9901.");

    let client_map = ClientMap::new(Mutex::new(HashMap::new()));


    while let Ok((stream, address)) = server_socket.accept().await {
        tokio::spawn(accept_and_set_up_client(
            client_map.clone(),
            stream,
            address,
        ));
    }

    Ok(())
}
