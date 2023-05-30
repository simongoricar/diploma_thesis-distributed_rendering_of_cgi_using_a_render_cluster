use crate::messages::WebSocketMessage;

pub trait Message {
    fn type_name() -> &'static str;
}

pub trait IntoWebSocketMessage {
    fn into_ws_message(self) -> WebSocketMessage;
}

impl<M> IntoWebSocketMessage for M
where
    M: Into<WebSocketMessage>,
{
    fn into_ws_message(self) -> WebSocketMessage {
        self.into()
    }
}
