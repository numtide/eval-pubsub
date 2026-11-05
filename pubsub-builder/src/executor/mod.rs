pub mod eval;

use std::path::PathBuf;

use pubsub_proto::pubsub as proto;

use crate::output::OutputSink;

#[derive(Debug, Clone, PartialEq)]
pub enum Conclusion {
    Success(proto::server::conclude_job_request::Result),
    Failure(String),
}

impl Conclusion {
    pub fn into_request_parts(
        self,
    ) -> (
        proto::common::JobStatus,
        Option<proto::server::conclude_job_request::Result>,
    ) {
        match self {
            Self::Success(result) => (
                proto::common::JobStatus {
                    result: Some(proto::common::job_status::Result::Success(
                        proto::common::job_status::Success {},
                    )),
                },
                Some(result),
            ),
            Self::Failure(_) => (
                proto::common::JobStatus {
                    result: Some(proto::common::job_status::Result::Failure(
                        proto::common::job_status::Failure {},
                    )),
                },
                None,
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Executor {
    nix_eval_jobs: PathBuf,
}

impl Executor {
    pub fn new(nix_eval_jobs: PathBuf) -> Self {
        Self { nix_eval_jobs }
    }

    pub fn supports(&self, job: &proto::common::AnonymousJob) -> bool {
        matches!(
            job.r#type,
            Some(proto::common::anonymous_job::Type::Eval(_))
        )
    }

    pub async fn run(&self, job: proto::common::AnonymousJob, output: &OutputSink) -> Conclusion {
        match job.r#type {
            Some(proto::common::anonymous_job::Type::Eval(eval_job)) => {
                eval::run(&self.nix_eval_jobs, eval_job, output).await
            }
            Some(proto::common::anonymous_job::Type::Build(_)) => {
                Conclusion::Failure("build jobs are not supported yet".to_owned())
            }
            None => Conclusion::Failure("job has no type".to_owned()),
        }
    }
}
