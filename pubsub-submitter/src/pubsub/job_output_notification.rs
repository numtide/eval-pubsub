use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::pubsub::db_connector::PgNotifiable;

#[derive(Serialize, Deserialize, Debug)]
pub struct JobOutputNotification {
    pub job_uuid: Uuid,
    pub job_output_id: i64,
}

impl PgNotifiable for JobOutputNotification {
    fn channel() -> String {
        "job_output".to_string()
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

impl JobOutputNotification {
    pub fn new(job_uuid: Uuid, job_output_id: i64) -> Self {
        Self {
            job_uuid,
            job_output_id,
        }
    }
}
