use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct MessageRequestID(u64);

impl MessageRequestID {
    #[inline]
    pub fn generate() -> Self {
        Self(rand::random::<u64>())
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct OutgoingMessageID(u64);

impl OutgoingMessageID {
    #[inline]
    pub fn generate() -> Self {
        Self(rand::random::<u64>())
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}
