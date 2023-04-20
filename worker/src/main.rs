use log::info;
use miette::{IntoDiagnostic, Result};
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

async fn handle_server_connection(
    _stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
) {
    info!("Connected to server!");
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
