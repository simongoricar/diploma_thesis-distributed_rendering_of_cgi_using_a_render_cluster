use std::io::ErrorKind;
use std::{future, io};

use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures_util::future::try_join3;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{pin_mut, StreamExt, TryStreamExt};
use log::{info, warn};
use miette::{miette, Context, IntoDiagnostic, Result};
use shared::messages::handshake::WorkerHandshakeResponse;
use shared::messages::WebSocketMessage;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

async fn handle_incoming_server_messages(
    stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
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

async fn handle_outgoing_client_messages(
    ws_writer: SplitSink<
        WebSocketStream<MaybeTlsStream<TcpStream>>,
        tungstenite::Message,
    >,
    message_sender_channel_rx: UnboundedReceiver<tungstenite::Message>,
) -> Result<()> {
    message_sender_channel_rx
        .map(Ok)
        .forward(ws_writer)
        .await
        .into_diagnostic()?;

    Ok(())
}

async fn run_server_connection(
    message_sender: UnboundedSender<tungstenite::Message>,
    mut message_receiver: UnboundedReceiver<WebSocketMessage>,
) -> Result<()> {
    info!("Waiting for handshake request.");

    let handshake_request = {
        let next_message = message_receiver
            .next()
            .await
            .ok_or_else(|| miette!("Could not receive initial message."))?;

        match next_message {
            WebSocketMessage::MasterHandshakeRequest(request) => request,
            _ => {
                return Err(miette!(
                    "Invalid handshake request: not a handshake_request!"
                ));
            }
        }
    };

    info!("Got handshake request, sending response.");

    let handshake_response: WebSocketMessage =
        WorkerHandshakeResponse::new("1.0.0").into();
    handshake_response
        .send(&message_sender)
        .wrap_err_with(|| miette!("Could not send handshake response."))?;

    info!("Sent handshake response, waiting for acknowledgement.");

    let handshake_ack = {
        let next_message = message_receiver
            .next()
            .await
            .ok_or_else(|| miette!("Could not receive ack."))?;

        match next_message {
            WebSocketMessage::MasterHandshakeAcknowledgement(ack) => ack,
            _ => {
                return Err(miette!(
                    "Invalid handshake ack: not a handshake_acknowledgement"
                ));
            }
        }
    };

    if !handshake_ack.ok {
        warn!("Handshake acknowledgement received, but ok==false ?!");
        return Err(miette!(
            "Server rejected connection (ACK not ok)."
        ));
    }

    info!("Server acknowledged handshake - fully connected!");

    Ok(())
}

async fn handle_server_connection(
    ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
) -> Result<()> {
    info!("Connected to server!");

    let (ws_write, ws_read) = ws_stream.split();

    let (ws_sender_tx, ws_sender_rx) =
        futures_channel::mpsc::unbounded::<tungstenite::Message>();
    let (ws_receiver_tx, ws_receiver_rx) =
        futures_channel::mpsc::unbounded::<WebSocketMessage>();

    let send_queued_messages =
        handle_outgoing_client_messages(ws_write, ws_sender_rx);
    let receive_and_parse_messages =
        handle_incoming_server_messages(ws_read, ws_receiver_tx);

    let server_connection_handler =
        run_server_connection(ws_sender_tx, ws_receiver_rx);

    pin_mut!(
        send_queued_messages,
        receive_and_parse_messages,
        server_connection_handler
    );

    try_join3(
        send_queued_messages,
        receive_and_parse_messages,
        server_connection_handler,
    )
    .await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::try_init().into_diagnostic()?;

    let server_address =
        url::Url::parse("ws://127.0.0.1:9901").into_diagnostic()?;

    info!("Connecting to 127.0.0.1:9901...");
    let (stream, _) = connect_async(server_address).await.into_diagnostic()?;
    handle_server_connection(stream).await?;

    Ok(())
}
