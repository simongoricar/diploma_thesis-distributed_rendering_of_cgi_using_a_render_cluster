mod cli;
mod websockets;

use std::collections::HashMap;
use std::sync::Mutex;

use log::info;
use miette::{IntoDiagnostic, Result};
use tokio::net::TcpListener;

use crate::websockets::{accept_and_set_up_client, ClientMap};


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
