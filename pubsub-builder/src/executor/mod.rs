pub mod eval;

use std::path::PathBuf;

use pubsub_nar_copy::nix_client;
use pubsub_proto::pubsub as proto;
use tonic::transport::Channel;

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
        proto::server::conclude_job_request::JobStatus,
        Option<proto::server::conclude_job_request::Result>,
    ) {
        match self {
            Self::Success(result) => (
                proto::server::conclude_job_request::JobStatus::Success(
                    proto::server::conclude_job_request::Success {},
                ),
                Some(result),
            ),
            Self::Failure(_) => (
                proto::server::conclude_job_request::JobStatus::Failure(
                    proto::server::conclude_job_request::Failure {},
                ),
                None,
            ),
        }
    }
}

#[derive(Clone)]
pub struct Executor {
    nix_eval_jobs: PathBuf,
    nix_client_pool: nix_client::Pool,
    nar_copy_clients: Vec<pubsub_nar_copy::Client<Channel>>,
}

impl Executor {
    pub fn new<C>(
        nix_eval_jobs: PathBuf,
        nix_client_pool: nix_client::Pool,
        nar_copy_client: C,
    ) -> Self
    where
        C: Into<Vec<pubsub_nar_copy::Client<Channel>>>,
    {
        Self {
            nix_eval_jobs,
            nix_client_pool,
            nar_copy_clients: nar_copy_client.into(),
        }
    }

    pub fn supports(&self, job: &proto::common::AnonymousJob) -> bool {
        matches!(
            job.r#type,
            Some(proto::common::anonymous_job::Type::Eval(_))
        )
    }

    pub async fn run(
        &mut self,
        job: proto::common::AnonymousJob,
        output: &OutputSink,
    ) -> Conclusion {
        match job.r#type {
            Some(proto::common::anonymous_job::Type::Eval(eval_job)) => {
                eval::run(self, eval_job, output).await
            }
            Some(proto::common::anonymous_job::Type::Build(_)) => {
                Conclusion::Failure("build jobs are not supported yet".to_owned())
            }
            None => Conclusion::Failure("job has no type".to_owned()),
        }
    }
}
