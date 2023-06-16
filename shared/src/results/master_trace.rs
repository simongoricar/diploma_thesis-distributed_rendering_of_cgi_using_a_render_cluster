use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use serde_with::TimestampSecondsWithFrac;

#[serde_as]
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct MasterTrace {
    #[serde_as(as = "TimestampSecondsWithFrac<f64>")]
    pub job_start_time: SystemTime,

    #[serde_as(as = "TimestampSecondsWithFrac<f64>")]
    pub job_finish_time: SystemTime,
}

impl MasterTrace {
    pub fn new(start_time: SystemTime, finish_time: SystemTime) -> Self {
        Self {
            job_start_time: start_time,
            job_finish_time: finish_time,
        }
    }
}
