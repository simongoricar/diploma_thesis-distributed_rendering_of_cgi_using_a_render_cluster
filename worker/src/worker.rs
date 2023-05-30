use std::io;
use std::io::ErrorKind;
use std::sync::Arc;

use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures_util::future::try_join3;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{future, pin_mut, StreamExt, TryStreamExt};
use log::{info, warn};
use miette::{miette, Context, IntoDiagnostic, Result};
use shared::messages::handshake::WorkerHandshakeResponse;
use shared::messages::WebSocketMessage;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async,
    tungstenite,
    MaybeTlsStream,
    WebSocketStream,
};

use crate::jobs::{BlenderJobRunner, WorkerAutomaticQueue};


pub struct ClientWorker {
    ws_sink: SplitSink<
        WebSocketStream<MaybeTlsStream<TcpStream>>,
        tungstenite::Message,
    >,
    ws_stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,

    sender_tx: UnboundedSender<tungstenite::Message>,
    sender_rx: UnboundedReceiver<tungstenite::Message>,

    receiver_tx: UnboundedSender<WebSocketMessage>,
    receiver_rx: UnboundedReceiver<WebSocketMessage>,
}

impl ClientWorker {
    pub async fn connect() -> Result<Self> {
        let server_address =
            url::Url::parse("ws://127.0.0.1:9901").into_diagnostic()?;

        let (stream, _) =
            connect_async(server_address).await.into_diagnostic()?;

        let (ws_write, ws_read) = stream.split();

        let (ws_sender_tx, ws_sender_rx) =
            futures_channel::mpsc::unbounded::<tungstenite::Message>();
        let (ws_receiver_tx, ws_receiver_rx) =
            futures_channel::mpsc::unbounded::<WebSocketMessage>();

        Ok(Self {
            ws_sink: ws_write,
            ws_stream: ws_read,
            sender_tx: ws_sender_tx,
            sender_rx: ws_sender_rx,
            receiver_tx: ws_receiver_tx,
            receiver_rx: ws_receiver_rx,
        })
    }

    pub async fn run_indefinitely(
        self,
        blender_runner: BlenderJobRunner,
    ) -> Result<()> {
        let send_queued_messages =
            Self::forward_outgoing_client_messages(self.ws_sink, self.sender_rx);
        let parse_and_forward_incoming_messages =
            Self::forward_incoming_server_messages(
                self.ws_stream,
                self.receiver_tx,
            );

        let connection_handler = Self::perform_handshake_and_run_connection(
            self.sender_tx,
            self.receiver_rx,
            blender_runner,
        );

        pin_mut!(
            send_queued_messages,
            parse_and_forward_incoming_messages,
            connection_handler
        );

        try_join3(
            send_queued_messages,
            parse_and_forward_incoming_messages,
            connection_handler,
        )
        .await
        .wrap_err_with(|| miette!("Errored while running connection."))?;

        Ok(())
    }

    async fn forward_outgoing_client_messages(
        ws_sink: SplitSink<
            WebSocketStream<MaybeTlsStream<TcpStream>>,
            tungstenite::Message,
        >,
        message_sender_rx: UnboundedReceiver<tungstenite::Message>,
    ) -> Result<()> {
        message_sender_rx
            .map(Ok)
            .forward(ws_sink)
            .await
            .into_diagnostic()
            .wrap_err_with(|| {
                miette!(
                    "Could not forward outgoing message through the WebSocket."
                )
            })?;

        Ok(())
    }

    async fn forward_incoming_server_messages(
        ws_stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        receiver_queue: UnboundedSender<WebSocketMessage>,
    ) -> Result<()> {
        ws_stream
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
            .into_diagnostic()
            .wrap_err_with(|| miette!("Could not parse/forward incoming WebSocket message."))?;

        Ok(())
    }

    async fn perform_handshake_and_run_connection(
        message_sender: UnboundedSender<tungstenite::Message>,
        mut message_receiver: UnboundedReceiver<WebSocketMessage>,
        blender_runner: BlenderJobRunner,
    ) -> Result<()> {
        /*
         * Step 1: perform handshake
         * (server request, worker response, server acknowledgement).
         */
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

        info!(
            "Got handshake request (server version is {}), sending response.",
            handshake_request.server_version
        );

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

        /*
         * Step 2: until connection is dropped, receive requests, perform renders and report progress
         */
        let message_sender_arc = Arc::new(message_sender);

        let queue = WorkerAutomaticQueue::new(
            blender_runner,
            message_sender_arc.clone(),
        );
        queue.start().await;

        // TODO
        loop {
            let next_message = message_receiver
                .next()
                .await
                .ok_or_else(|| miette!("Could not receive next message."))?;

            match next_message {
                WebSocketMessage::MasterFrameQueueAddRequest(request) => {
                    info!(
                        "New frame queued: {}, frame {}",
                        request.job.job_name, request.frame_index
                    );
                    queue.queue_frame(request.job, request.frame_index).await;
                }
                WebSocketMessage::MasterFrameQueueRemoveRequest(request) => {
                    info!(
                        "Frame removed from queue: {}, frame {}",
                        request.job_name, request.frame_index
                    );
                    queue
                        .unqueue_frame(request.job_name, request.frame_index)
                        .await;
                }
                _ => {
                    return Err(miette!("Invalid WebSocket message."));
                }
            }
        }
    }
}
