use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Copy, Clone, Serialize, Deserialize, Debug)]
pub struct ConcludedJobNotification {
    pub job_uuid: Uuid,
}

impl ConcludedJobNotification {
    pub fn new(job_uuid: Uuid) -> Self {
        Self { job_uuid }
    }

    pub fn to_json(&self) -> Result<String, serde_json::error::Error> {
        serde_json::to_string(self)
    }

    pub fn from_json<S>(string: S) -> Result<Self, serde_json::error::Error>
    where
        S: AsRef<str>,
    {
        serde_json::from_str(string.as_ref())
    }
}
