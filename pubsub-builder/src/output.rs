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

    pub async fn line<S>(&self, line: S)
    where
        S: Into<String>,
    {
        if let Err(err) = self
            .submitter
            .job_output(self.job_id, vec![line.into()])
            .await
        {
            log::warn!("failed to send output of job {}: {}", self.job_id, err);
        }
    }
}
