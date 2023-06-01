use std::fmt::Display;

use log::{debug, error, info, trace, warn};

#[derive(Clone)]
pub struct Logger {
    name: String,
}

impl Logger {
    pub fn new<S: Into<String>>(name: S) -> Self {
        Self { name: name.into() }
    }

    #[inline]
    fn format(&self, contents: String) -> String {
        format!("{} - {}", self.name, contents)
    }

    #[inline]
    pub fn error<D: Display>(&self, contents: D) {
        error!("{}", self.format(contents.to_string()));
    }

    #[inline]
    pub fn warn<D: Display>(&self, contents: D) {
        warn!("{}", self.format(contents.to_string()));
    }

    #[inline]
    pub fn info<D: Display>(&self, contents: D) {
        info!("{}", self.format(contents.to_string()));
    }

    #[inline]
    pub fn debug<D: Display>(&self, contents: D) {
        debug!("{}", self.format(contents.to_string()));
    }

    #[inline]
    pub fn trace<D: Display>(&self, contents: D) {
        trace!("{}", self.format(contents.to_string()));
    }
}
