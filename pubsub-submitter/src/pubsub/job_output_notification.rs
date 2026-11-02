use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug)]
pub struct JobOutputNotification {
    pub job_uuid: Uuid,
    pub job_output_id: i64,
}
