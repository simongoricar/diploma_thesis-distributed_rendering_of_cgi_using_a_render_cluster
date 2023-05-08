use std::future;
use std::time::Duration;

use futures_channel::mpsc::UnboundedSender;
use futures_util::stream::SplitStream;
use futures_util::{StreamExt, TryStreamExt};
use log::info;
use miette::{IntoDiagnostic, Result};
use tokio::net::TcpStream;
use tokio::time::sleep;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

async fn handle_incoming_server_messages(
    stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    sender_queue: UnboundedSender<Message>,
) -> Result<()> {
    stream
        .try_for_each(|_message| {
            // TODO

            future::ready(Ok(()))
        })
        .await
        .into_diagnostic()?;

    Ok(())
}

async fn handle_server_connection(
    ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
) {
    info!("Connected to server!");

    let (ws_write, ws_read) = ws_stream.split();



    sleep(Duration::from_secs(10)).await;
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::try_init().into_diagnostic()?;

    let server_address =
        url::Url::parse("ws://127.0.0.1:9901").into_diagnostic()?;

    info!("Connecting to 127.0.0.1:9901...");
    let (stream, _) = connect_async(server_address).await.into_diagnostic()?;
    handle_server_connection(stream).await;

    Ok(())
}
