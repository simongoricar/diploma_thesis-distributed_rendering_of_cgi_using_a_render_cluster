use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use futures_channel::mpsc::UnboundedSender;
use log::info;
use miette::{IntoDiagnostic, Result};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;

type ClientMap = Arc<Mutex<HashMap<SocketAddr, UnboundedSender<Message>>>>;

async fn handle_client(
    _client_map: ClientMap,
    _stream: TcpStream,
    address: SocketAddr,
) {
    info!("New client: {:?}", address);
    // TODO
}

// async fn enqueue_incoming_messages(
//     stream_reader: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
//     sender: UnboundedSender<Message>,
// ) {
//     stream_reader
//         .for_each(|message| async {
//             let message = message.unwrap();
//
//             sender
//                 .unbounded_send(message)
//                 .expect("Could not forward incoming message.");
//         })
//         .await;
// }

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::try_init().into_diagnostic()?;

    let server_socket =
        TcpListener::bind("0.0.0.0:9901").await.into_diagnostic()?;
    info!("Server running at 0.0.0.0:9901.");

    let client_map = ClientMap::new(Mutex::new(HashMap::new()));


    while let Ok((stream, address)) = server_socket.accept().await {
        tokio::spawn(handle_client(client_map.clone(), stream, address));
    }

    // let (message_tx, message_rx) = futures_channel::mpsc::unbounded::<Message>();

    // let (stream, _) = connect_async(test_client_address)
    //     .await
    //     .into_diagnostic()
    //     .wrap_err_with(|| miette!("Could not connect to test client."))?;
    // let (stream_write, stream_read) = stream.split();

    // let message_receiver = enqueue_incoming_messages(stream_read, message_tx);
    // let connection_handler = handle_client(stream_write, message_rx);

    // TODO
    // future::join(message_receiver, connection_handler).await;

    Ok(())
}
