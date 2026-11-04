use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::pubsub::db_connector::PgNotifiable;

#[derive(Copy, Clone, Serialize, Deserialize, Debug)]
pub struct PendingJobNotification {
    pub job_uuid: Uuid,
}

impl PgNotifiable for PendingJobNotification {
    fn channel() -> String {
        "pending_job".to_string()
    }

    fn to_json(&self) -> Result<String, serde_json::error::Error> {
        serde_json::to_string(self)
    }

    fn from_json<S>(string: S) -> Result<Self, serde_json::error::Error>
    where
        S: AsRef<str>,
    {
        serde_json::from_str(string.as_ref())
    }
}

impl PendingJobNotification {
    pub fn new(job_uuid: Uuid) -> Self {
        Self { job_uuid }
    }
}
