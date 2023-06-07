use std::sync::Arc;
use std::time::Duration;

use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use log::{debug, trace};
use miette::{miette, Context, IntoDiagnostic, Result};
use shared::cancellation::CancellationToken;
use shared::messages::{OutgoingMessage, SenderHandle};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

pub struct MasterSender {
    sender_channel: Arc<UnboundedSender<OutgoingMessage>>,

    pub task_join_handle: JoinHandle<Result<()>>,
}

impl MasterSender {
    pub fn new(
        websocket_sink: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        global_cancellation_token: CancellationToken,
    ) -> Self {
        let (message_send_queue_tx, message_send_queue_rx) =
            futures_channel::mpsc::unbounded::<OutgoingMessage>();

        let join_handle = tokio::spawn(Self::run(
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
        mut websocket_sink: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        mut message_send_queue_receiver: UnboundedReceiver<OutgoingMessage>,
        global_cancellation_token: CancellationToken,
    ) -> Result<()> {
        debug!("Running task loop: forwarding incoming messages through WebSocket connection.");

        loop {
            let potential_message = tokio::time::timeout(
                Duration::from_secs(2),
                message_send_queue_receiver.next(),
            )
            .await;

            if global_cancellation_token.is_cancelled() {
                trace!("Stopping outgoing messages sender (worker stopping).");
                break;
            }

            if let Ok(outgoing_message) = potential_message {
                let outgoing_message = outgoing_message
                    .ok_or_else(|| miette!("Can't get outgoing message from queue channel!"))?;

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
