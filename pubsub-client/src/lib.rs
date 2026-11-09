pub mod error;

use std::io::ErrorKind::StaleNetworkFileHandle;

use futures::{Stream, StreamExt as _};
use pubsub_proto::pubsub::{
    client::{self as client, pub_sub_client_client::PubSubClientClient},
    common,
};
use serde::{Deserialize, Serialize};
use tonic::{Request, Status, transport::Channel};
use uuid::Uuid;

use crate::error::GrpcError;

type Error = error::Error;

#[derive(Clone)]
pub struct Client<C> {
    tonic_client: PubSubClientClient<C>,
}

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq, Debug)]
pub enum JobFilter {
    Pending,
    Picked,
    Failed,
    Succeeded,
}

impl TryFrom<JobFilter> for client::list_jobs_request::Filter {
    type Error = Error;

    fn try_from(value: JobFilter) -> Result<Self, Self::Error> {
        match value {
            JobFilter::Pending => Ok(client::list_jobs_request::Filter::Pending(
                client::list_jobs_request::FilterPending {},
            )),
            JobFilter::Picked => Ok(client::list_jobs_request::Filter::Picked(
                client::list_jobs_request::FilterPicked {},
            )),
            JobFilter::Failed => Ok(client::list_jobs_request::Filter::Failed(
                client::list_jobs_request::FilterFailed {},
            )),
            JobFilter::Succeeded => Ok(client::list_jobs_request::Filter::Succeeded(
                client::list_jobs_request::FilterSucceeded {},
            )),
        }
    }
}

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq, Debug)]
pub enum JobStatus {
    Pending,
    Picked,
    Failed { builder_uuid: Uuid },
    Succeeded { builder_uuid: Uuid },
}

impl TryFrom<common::JobStatus> for JobStatus {
    type Error = Error;

    fn try_from(value: common::JobStatus) -> Result<Self, Self::Error> {
        match value.status.ok_or(GrpcError::field_missing(&["status"]))? {
            common::job_status::Status::Pending(_) => Ok(JobStatus::Pending),
            common::job_status::Status::Picked(_) => Ok(JobStatus::Picked),
            common::job_status::Status::Concluded(concluded) => concluded.try_into(),
        }
    }
}

impl TryFrom<common::job_status::Concluded> for JobStatus {
    type Error = Error;

    fn try_from(value: common::job_status::Concluded) -> Result<Self, Self::Error> {
        match value
            .conclusion
            .ok_or(GrpcError::field_missing(&["conclusion"]))?
        {
            common::job_status::concluded::Conclusion::Success(
                common::job_status::concluded::Success { builder_uuid },
            ) => Ok(JobStatus::Succeeded {
                builder_uuid: builder_uuid
                    .ok_or(GrpcError::field_missing(&["conclusion", "builder_uuid"]))?
                    .into(),
            }),
            common::job_status::concluded::Conclusion::Failure(
                common::job_status::concluded::Failure { builder_uuid },
            ) => Ok(JobStatus::Failed {
                builder_uuid: builder_uuid
                    .ok_or(GrpcError::field_missing(&["conclusion", "builder_uuid"]))?
                    .into(),
            }),
        }
    }
}

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq, Debug)]
pub enum EvalJob {
    Flake {
        flake_ref: String,
        attrs: Vec<String>,
    },
}

impl From<EvalJob> for common::EvalJob {
    fn from(value: EvalJob) -> Self {
        match value {
            EvalJob::Flake { flake_ref, attrs } => Self {
                r#type: Some(common::eval_job::Type::Flake(common::eval_job::Flake {
                    flake_ref,
                    attrs,
                })),
            },
        }
    }
}

impl TryFrom<common::EvalJob> for EvalJob {
    type Error = Error;

    fn try_from(value: common::EvalJob) -> Result<Self, Self::Error> {
        let value = value
            .r#type
            .ok_or(GrpcError::field_missing(&["type", "type"]))?;
        match value {
            common::eval_job::Type::Flake(flake) => Ok(EvalJob::Flake {
                flake_ref: flake.flake_ref,
                attrs: flake.attrs,
            }),
        }
    }
}

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq, Debug)]
pub struct BuildJob {
    derivation: String,
}

impl From<BuildJob> for common::BuildJob {
    fn from(value: BuildJob) -> Self {
        Self {
            derivation: value.derivation,
        }
    }
}

impl TryFrom<common::BuildJob> for BuildJob {
    type Error = Error;

    fn try_from(value: common::BuildJob) -> Result<Self, Self::Error> {
        Ok(BuildJob {
            derivation: value.derivation,
        })
    }
}

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq, Debug)]
pub enum AnonymousJob {
    EvalJob(EvalJob),
    BuildJob(BuildJob),
}

impl From<AnonymousJob> for common::AnonymousJob {
    fn from(value: AnonymousJob) -> Self {
        match value {
            AnonymousJob::EvalJob(eval_job) => common::AnonymousJob {
                r#type: Some(common::anonymous_job::Type::Eval(eval_job.into())),
            },
            AnonymousJob::BuildJob(build_job) => common::AnonymousJob {
                r#type: Some(common::anonymous_job::Type::Build(build_job.into())),
            },
        }
    }
}

impl TryFrom<common::AnonymousJob> for AnonymousJob {
    type Error = Error;

    fn try_from(value: common::AnonymousJob) -> Result<Self, Self::Error> {
        match value.r#type.ok_or(GrpcError::field_missing(&["type"]))? {
            common::anonymous_job::Type::Eval(eval_job) => Ok(Self::EvalJob(eval_job.try_into()?)),
            common::anonymous_job::Type::Build(build_job) => {
                Ok(Self::BuildJob(build_job.try_into()?))
            }
        }
    }
}

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq, Debug)]
pub struct BuilderInformation {
    pub uuid: Uuid,
    pub system: String,
    pub smt_core_count: i32,
    pub total_memory: u64,
    pub store_uri: String,
}

impl TryFrom<common::BuilderInformation> for BuilderInformation {
    type Error = Error;

    fn try_from(value: common::BuilderInformation) -> Result<Self, Self::Error> {
        Ok(Self {
            uuid: value
                .uuid
                .ok_or(GrpcError::field_missing(&["uuid"]))?
                .into(),
            system: value.system,
            smt_core_count: value.smt_core_count,
            total_memory: value.total_memory,
            store_uri: value.store_uri,
        })
    }
}

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq, Debug)]
pub struct Builder {
    pub information: BuilderInformation,
    pub active: bool,
}

impl TryFrom<common::Builder> for Builder {
    type Error = Error;

    fn try_from(value: common::Builder) -> Result<Self, Self::Error> {
        Ok(Self {
            information: value
                .information
                .ok_or(GrpcError::field_missing(&["information"]))?
                .try_into()?,
            active: value.active,
        })
    }
}

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq, Debug)]
pub struct Job {
    pub uuid: Uuid,
    pub anonymous: AnonymousJob,
}

impl TryFrom<common::Job> for Job {
    type Error = Error;

    fn try_from(value: common::Job) -> Result<Self, Self::Error> {
        Ok(Self {
            uuid: value
                .job_id
                .ok_or(GrpcError::field_missing(&["uuid"]))?
                .into(),
            anonymous: value
                .anonymous_job
                .ok_or(GrpcError::field_missing(&["anonymous"]))
                .map_err(Into::<GrpcError>::into)?
                .try_into()?,
        })
    }
}

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq, Debug)]
pub struct JobWithStatus {
    pub job: Job,
    pub status: JobStatus,
}

impl TryFrom<common::JobWithStatus> for JobWithStatus {
    type Error = Error;

    fn try_from(value: common::JobWithStatus) -> Result<Self, Error> {
        Ok(Self {
            job: value
                .job
                .ok_or(GrpcError::field_missing(&["job"]))?
                .try_into()?,
            status: value
                .status
                .ok_or(GrpcError::field_missing(&["status"]))?
                .try_into()?,
        })
    }
}

impl Client<Channel> {
    pub async fn new<S>(address: S) -> Result<Self, Error>
    where
        S: AsRef<str>,
    {
        PubSubClientClient::connect(address.as_ref().to_owned())
            .await
            .map(|tonic_client| Client { tonic_client })
            .map_err(Into::<GrpcError>::into)
            .map_err(Into::into)
    }
    pub async fn submit_job(&mut self, anonymous_job: AnonymousJob) -> Result<Uuid, Error> {
        let response = self
            .tonic_client
            .submit_job(Request::new(client::SubmitJobRequest {
                job: Some(anonymous_job.into()),
            }))
            .await
            .map_err(Into::<GrpcError>::into)?;

        Ok(response.into_inner().job_id.unwrap().into())
    }
    pub async fn tail_job(
        &mut self,
        job_uuid: Uuid,
        follow: bool,
        end: bool,
        number: i64,
    ) -> Result<impl Stream<Item = Result<String, Status>> + use<>, Error> {
        let stream = self
            .tonic_client
            .tail_job(Request::new(client::TailJobRequest {
                job_id: Some(job_uuid.into()),
                follow,
                end,
                number,
            }))
            .await
            .map_err(Into::<GrpcError>::into)?
            .into_inner()
            .map(|response| response.map(|response| response.line));
        Ok(stream)
    }
    pub async fn wait_job(&mut self, job_uuid: Uuid) -> Result<JobStatus, Error> {
        let mut stream = self
            .tonic_client
            .wait_job(Request::new(client::WaitJobRequest {
                job_id: Some(job_uuid.into()),
            }))
            .await
            .map_err(Into::<GrpcError>::into)?
            .into_inner();

        match stream.next().await {
            Some(response) => Ok(response
                .map_err(Into::<GrpcError>::into)?
                .job_status
                .ok_or(GrpcError::field_missing(&["job_status"]))?
                .try_into()?),
            None => todo!(),
        }
    }
    pub async fn list_jobs(
        &mut self,
        page: i32,
        count: i32,
        filter: Option<JobFilter>,
    ) -> Result<(Vec<JobWithStatus>, i32), Error> {
        let response = self
            .tonic_client
            .list_jobs(Request::new(client::ListJobsRequest {
                page,
                count,
                filter: filter.map(TryInto::try_into).transpose()?,
            }))
            .await
            .map_err(Into::<GrpcError>::into)?
            .into_inner();
        let jobs = response
            .jobs
            .into_iter()
            .map(|job| job.try_into())
            .collect::<Result<Vec<_>, _>>()?;
        Ok((jobs, response.total))
    }
    pub async fn list_builders(
        &mut self,
        page: i32,
        count: i32,
    ) -> Result<(Vec<Builder>, i32), Error> {
        let response = self
            .tonic_client
            .list_builders(Request::new(client::ListBuildersRequest { page, count }))
            .await
            .map_err(Into::<GrpcError>::into)?
            .into_inner();
        let builders = response
            .builders
            .into_iter()
            .map(|builder| builder.try_into())
            .collect::<Result<Vec<_>, _>>()?;
        Ok((builders, response.total))
    }
}
