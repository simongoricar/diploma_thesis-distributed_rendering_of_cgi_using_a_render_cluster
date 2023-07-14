use std::sync::Arc;
use std::time::Duration;

use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures_util::StreamExt;
use miette::{miette, Context, IntoDiagnostic, Result};
use shared::cancellation::CancellationToken;
use shared::messages::{OutgoingMessage, SenderHandle};
use tokio::task::JoinHandle;
use tracing::{debug, error};

use crate::connection::ReconnectingWebSocketClient;

pub struct MasterSender {
    sender_channel: Arc<UnboundedSender<OutgoingMessage>>,

    pub task_join_handle: JoinHandle<Result<()>>,
}

impl MasterSender {
    pub fn new(
        client: Arc<ReconnectingWebSocketClient>,
        global_cancellation_token: CancellationToken,
    ) -> Self {
        let (message_send_queue_tx, message_send_queue_rx) =
            futures_channel::mpsc::unbounded::<OutgoingMessage>();

        let join_handle = tokio::spawn(Self::run(
            client,
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
        websocket_client: Arc<ReconnectingWebSocketClient>,
        mut message_send_queue_receiver: UnboundedReceiver<OutgoingMessage>,
        global_cancellation_token: CancellationToken,
    ) -> Result<()> {
        debug!("Running task loop: forwarding incoming messages through WebSocket connection.");

        loop {
            if global_cancellation_token.is_cancelled() {
                debug!("Stopping outgoing messages sender (worker stopping).");
                break;
            }


            let potential_message = tokio::time::timeout(
                Duration::from_secs(2),
                message_send_queue_receiver.next(),
            )
            .await;

            if let Ok(outgoing_message) = potential_message {
                let outgoing_message = outgoing_message
                    .ok_or_else(|| miette!("Can't get outgoing message from queue channel!"))?;

                debug!(
                    message_id = outgoing_message.id.as_u64(),
                    "Sending message.",
                );

                match websocket_client
                    .send_message(outgoing_message.message)
                    .await
                {
                    Ok(_) => {
                        debug!(
                            message_id = outgoing_message.id.as_u64(),
                            "Message sent and oneshot triggered.",
                        );
                        let _ = outgoing_message.oneshot_sender.send(());
                    }
                    Err(error) => {
                        error!(
                            error = ?error,
                            "Failed to send message through the reconnecting socket (oneshot not triggered)."
                        );

                        return Err(error).wrap_err_with(|| miette!("Failed to send message."));
                    }
                };
            }
        }

        Ok(())
    }
}
