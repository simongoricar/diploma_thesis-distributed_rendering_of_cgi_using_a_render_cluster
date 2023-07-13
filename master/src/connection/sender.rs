use std::sync::Arc;
use std::time::Duration;

use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures_util::StreamExt;
use miette::{miette, Context, IntoDiagnostic, Result};
use shared::cancellation::CancellationToken;
use shared::messages::{OutgoingMessage, SenderHandle};
use tokio::task::JoinHandle;

use crate::cluster::ReconnectableClientConnection;
use crate::connection::worker_logger::WorkerLogger;

pub struct WorkerSender {
    sender_channel: Arc<UnboundedSender<OutgoingMessage>>,

    pub task_join_handle: JoinHandle<Result<()>>,
}

impl WorkerSender {
    pub fn new(
        connection: Arc<ReconnectableClientConnection>,
        cancellation_token: CancellationToken,
        logger: WorkerLogger,
    ) -> Self {
        // TODO Update to use reconnectable connection
        let (message_send_queue_tx, message_send_queue_rx) =
            futures_channel::mpsc::unbounded::<OutgoingMessage>();

        let join_handle = tokio::spawn(Self::run(
            connection,
            message_send_queue_rx,
            logger,
            cancellation_token,
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
        connection: Arc<ReconnectableClientConnection>,
        mut message_send_queue_receiver: UnboundedReceiver<OutgoingMessage>,
        logger: WorkerLogger,
        global_cancellation_token: CancellationToken,
    ) -> Result<()> {
        logger.debug(
            "Starting task loop: forwarding messages from outgoing queue through WebSocket connection.",
        );

        loop {
            let potential_message = tokio::time::timeout(
                Duration::from_secs(2),
                message_send_queue_receiver.next(),
            )
            .await;

            if global_cancellation_token.is_cancelled() {
                logger.debug("Stopping outgoing message sender.");
                break;
            }

            if let Ok(outgoing_message) = potential_message {
                let outgoing_message = outgoing_message
                    .ok_or_else(|| miette!("Can't get outgoing message from channel!"))?;

                logger.trace(format!(
                    "Sending message with ID {}.",
                    outgoing_message.id.as_u64(),
                ));

                connection
                    .send_message(outgoing_message.message)
                    .await
                    .wrap_err_with(|| {
                        miette!("Could not send message, reconnectable connection failed.")
                    })?;

                let _ = outgoing_message.oneshot_sender.send(());

                logger.trace(format!(
                    "Message with ID {} sent.",
                    outgoing_message.id.as_u64(),
                ));
            }
        }

        Ok(())
    }
}
