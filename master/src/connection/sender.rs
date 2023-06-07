use std::sync::Arc;
use std::time::Duration;

use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use log::trace;
use miette::{miette, Context, IntoDiagnostic, Result};
use shared::cancellation::CancellationToken;
use shared::logger::Logger;
use shared::messages::{OutgoingMessage, WebSocketMessage};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

#[derive(Clone)]
pub struct SenderHandle {
    sender_channel: Arc<UnboundedSender<OutgoingMessage>>,
}

impl SenderHandle {
    fn from_channel(sender: Arc<UnboundedSender<OutgoingMessage>>) -> Self {
        Self {
            sender_channel: sender,
        }
    }

    pub async fn send_message<M: Into<WebSocketMessage>>(&self, message: M) -> Result<()> {
        let (message, sent_receiver) =
            OutgoingMessage::from_message(message.into().to_tungstenite_message()?);

        self.sender_channel
            .unbounded_send(message)
            .into_diagnostic()
            .wrap_err_with(|| miette!("Could not queue WebSocket message for sending."))?;

        sent_receiver
            .await
            .into_diagnostic()
            .wrap_err_with(|| miette!("Errored while waiting for WebSocket message to be sent."))?;

        Ok(())
    }
}

pub struct WorkerSender {
    sender_channel: Arc<UnboundedSender<OutgoingMessage>>,

    pub task_join_handle: JoinHandle<Result<()>>,
}

impl WorkerSender {
    pub fn new(
        websocket_sink: SplitSink<WebSocketStream<TcpStream>, Message>,
        global_cancellation_token: CancellationToken,
        logger: Arc<Logger>,
    ) -> Self {
        let (message_send_queue_tx, message_send_queue_rx) =
            futures_channel::mpsc::unbounded::<OutgoingMessage>();

        let join_handle = tokio::spawn(Self::run(
            logger,
            websocket_sink,
            message_send_queue_rx,
            global_cancellation_token,
        ));

        Self {
            sender_channel: Arc::new(message_send_queue_tx),
            task_join_handle: join_handle,
        }
    }

    pub fn sender_handle(&self) -> SenderHandle {
        SenderHandle::from_channel(self.sender_channel.clone())
    }

    pub async fn join(self) -> Result<()> {
        self.task_join_handle.await.into_diagnostic()?
    }

    /*
     * Private methods
     */

    async fn run(
        logger: Arc<Logger>,
        mut websocket_sink: SplitSink<WebSocketStream<TcpStream>, Message>,
        mut message_send_queue_receiver: UnboundedReceiver<OutgoingMessage>,
        global_cancellation_token: CancellationToken,
    ) -> Result<()> {
        logger.debug(
            "Running task loop: forwarding messages from channel through WebSocket connection.",
        );

        loop {
            let potential_message = tokio::time::timeout(
                Duration::from_secs(2),
                message_send_queue_receiver.next(),
            )
            .await;

            if global_cancellation_token.cancelled() {
                logger.trace("Stopping outgoing messages sender (cluster stopping).");
                break;
            }

            if let Ok(outgoing_message) = potential_message {
                let outgoing_message = outgoing_message
                    .ok_or_else(|| miette!("Can't get outgoing message from channel!"))?;

                trace!(
                    "Sending message: id={}",
                    outgoing_message.id.as_u64()
                );

                websocket_sink
                    .send(outgoing_message.message)
                    .await
                    .into_diagnostic()
                    .wrap_err_with(|| miette!("Could not send message, WebSocket sink failed."))?;

                let _ = outgoing_message.oneshot_sender.send(());
            }
        }

        Ok(())
    }
}
