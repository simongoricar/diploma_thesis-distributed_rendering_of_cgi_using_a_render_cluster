use std::fmt::Display;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use shared::messages::handshake::WorkerID;
use tracing::{debug, error, info, trace, warn};

use crate::cluster::{ClientConnectionStatus, ReconnectableClientConnection};


#[derive(Clone)]
pub struct WorkerLogger {
    worker_id: WorkerID,
    connection: Arc<ReconnectableClientConnection>,
}

impl WorkerLogger {
    pub fn new(connection: Arc<ReconnectableClientConnection>) -> Self {
        Self {
            worker_id: connection.worker_id,
            connection,
        }
    }

    #[inline]
    pub fn error<D: Display>(&self, contents: D) {
        let status = self.connection.connection_status.load(Ordering::SeqCst);

        match status {
            ClientConnectionStatus::Disconnected => {
                error!(
                    worker_id = %self.worker_id,
                    "{contents}"
                );
            }
            ClientConnectionStatus::Connected { address } => {
                error!(
                    worker_address = %address,
                    worker_id = %self.worker_id,
                    "{contents}"
                );
            }
        }
    }

    #[inline]
    pub fn warn<D: Display>(&self, contents: D) {
        let status = self.connection.connection_status.load(Ordering::SeqCst);

        match status {
            ClientConnectionStatus::Disconnected => {
                warn!(
                    worker_id = %self.worker_id,
                    "{contents}"
                );
            }
            ClientConnectionStatus::Connected { address } => {
                warn!(
                    worker_address = %address,
                    worker_id = %self.worker_id,
                    "{contents}"
                );
            }
        }
    }

    #[inline]
    pub fn info<D: Display>(&self, contents: D) {
        let status = self.connection.connection_status.load(Ordering::SeqCst);

        match status {
            ClientConnectionStatus::Disconnected => {
                info!(
                    worker_id = %self.worker_id,
                    "{contents}"
                );
            }
            ClientConnectionStatus::Connected { address } => {
                info!(
                    worker_address = %address,
                    worker_id = %self.worker_id,
                    "{contents}"
                );
            }
        }
    }

    #[inline]
    pub fn debug<D: Display>(&self, contents: D) {
        let status = self.connection.connection_status.load(Ordering::SeqCst);

        match status {
            ClientConnectionStatus::Disconnected => {
                debug!(
                    worker_id = %self.worker_id,
                    "{contents}"
                );
            }
            ClientConnectionStatus::Connected { address } => {
                debug!(
                    worker_address = %address,
                    worker_id = %self.worker_id,
                    "{contents}"
                );
            }
        }
    }

    #[inline]
    pub fn trace<D: Display>(&self, contents: D) {
        let status = self.connection.connection_status.load(Ordering::SeqCst);

        match status {
            ClientConnectionStatus::Disconnected => {
                trace!(
                    worker_id = %self.worker_id,
                    "{contents}"
                );
            }
            ClientConnectionStatus::Connected { address } => {
                trace!(
                    worker_address = %address,
                    worker_id = %self.worker_id,
                    "{contents}"
                );
            }
        }
    }
}
