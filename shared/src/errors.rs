use thiserror::Error;

#[derive(Error, Debug)]
pub enum WebSocketError {
    #[error("Decoding error: {0}")]
    DecodingError(String),
}
