use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

pub const DEFAULT_WEBSOCKET_CONFIG: WebSocketConfig = WebSocketConfig {
    max_send_queue: None,
    // Maximum message size is now 256 MB instead of 64 MB.
    max_message_size: Some(256 << 20),
    max_frame_size: Some(16 << 20),
    accept_unmasked_frames: false,
};

#[derive(Eq, PartialEq, Hash)]
pub enum WebSocketConnectionStatus {
    Reconnecting,
    Connected,
}
