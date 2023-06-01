use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
#[repr(transparent)]
pub struct MessageRequestID(u64);

impl MessageRequestID {
    #[inline]
    pub fn generate() -> Self {
        Self(rand::random::<u64>())
    }
}
