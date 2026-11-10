use std::time::SystemTime;

use pubsub_proto::pubsub::common::JobOutput;
use time::UtcDateTime;
use uuid::Uuid;

use crate::submitter::Submitter;

#[derive(Debug, Clone)]
pub struct OutputSink {
    submitter: Submitter,
    job_id: Uuid,
}

impl OutputSink {
    pub fn new(submitter: Submitter, job_id: Uuid) -> Self {
        Self { submitter, job_id }
    }

    pub async fn line<S>(&self, line: S, attr_path: Option<&Vec<String>>, time: UtcDateTime)
    where
        S: Into<String>,
    {
        if let Err(err) = self
            .submitter
            .job_output(
                self.job_id,
                vec![JobOutput {
                    line: line.into(),
                    time: Some(Into::<SystemTime>::into(time).into()),
                    attr_path: attr_path.map(|attr_path| {
                        pubsub_proto::pubsub::common::job_output::AttrPath {
                            attr_path: attr_path.clone(),
                        }
                    }),
                }],
            )
            .await
        {
            log::warn!("failed to send output of job {}: {}", self.job_id, err);
        }
    }
}
