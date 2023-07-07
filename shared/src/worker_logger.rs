use std::fmt::Display;

use tracing::{debug, error, info, trace, warn};

#[derive(Clone)]
pub struct WorkerLogger {
    worker_name: String,
}

impl WorkerLogger {
    pub fn new<S: Into<String>>(worker_name: S) -> Self {
        Self {
            worker_name: worker_name.into(),
        }
    }

    #[inline]
    pub fn error<D: Display>(&self, contents: D) {
        error!(
            worker = self.worker_name,
            "{}",
            contents.to_string()
        );
    }

    #[inline]
    pub fn warn<D: Display>(&self, contents: D) {
        warn!(
            worker = self.worker_name,
            "{}",
            contents.to_string()
        );
    }

    #[inline]
    pub fn info<D: Display>(&self, contents: D) {
        info!(
            worker = self.worker_name,
            "{}",
            contents.to_string()
        );
    }

    #[inline]
    pub fn debug<D: Display>(&self, contents: D) {
        debug!(
            worker = self.worker_name,
            "{}",
            contents.to_string()
        );
    }

    #[inline]
    pub fn trace<D: Display>(&self, contents: D) {
        trace!(
            worker = self.worker_name,
            "{}",
            contents.to_string()
        );
    }
}
