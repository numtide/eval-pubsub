pub mod error;

use std::{
    collections::HashSet,
    io::ErrorKind::StaleNetworkFileHandle,
    process::{Command, Stdio},
    sync::Arc,
};

use derive_where::derive_where;
use futures::{Stream, StreamExt as _};
use pubsub_nar_copy::{nix_client::NixClient, wire};
use pubsub_proto::pubsub::{
    client::{self as client, pub_sub_client_client::PubSubClientClient},
    common,
    nar_copy::nar_copy_client::NarCopyClient,
};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::UnixStream,
    sync::RwLock,
};
use tokio_util::io::SyncIoBridge;
use tonic::{Request, Status, transport::Channel};
use uuid::Uuid;

use crate::error::GrpcError;

type Error = error::Error;

#[derive_where(Clone)]
pub struct Client<C, RW>
where
    C: Clone,
{
    tonic_client_client: PubSubClientClient<C>,
    nar_copy_client: pubsub_nar_copy::Client<C>,
    nix_client: Arc<RwLock<NixClient<RW>>>,
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

impl EvalJob {
    async fn resolve<RW>(
        self,
        nix_client: &mut NixClient<RW>,
    ) -> Result<(common::EvalJob, HashSet<wire::StorePath>), Error>
    where
        RW: AsyncRead + AsyncWrite + Unpin + Send,
    {
        match self {
            Self::Flake { flake_ref, attrs } => {
                let mut child = Command::new("nix")
                    .args(&[
                        "--extra-experimental-features",
                        "nix-command flakes",
                        "flake",
                        "metadata",
                        "--json",
                    ])
                    .arg(&flake_ref)
                    .stdin(Stdio::null())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::inherit())
                    .spawn()
                    .unwrap();

                let stdout = child.stdout.take().expect("stdout is piped");

                let mut value: serde_json::Value = serde_json::from_reader(stdout).unwrap();
                let store_path: serde_json::Value = value["path"].take();
                let store_path: String = serde_json::from_value(store_path).unwrap();

                log::info!("resolved {} to {}", flake_ref, store_path);

                let eval_job = common::EvalJob {
                    r#type: Some(common::eval_job::Type::Flake(common::eval_job::Flake {
                        store_path: store_path.clone(),
                        attrs,
                    })),
                };

                let closure = nix_client
                    .get_closure(&store_path.try_into().unwrap())
                    .await?
                    .ok_or(Error::FailedToFetchClosure(flake_ref))?;

                Ok((eval_job, closure))
            }
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
                flake_ref: format!("path:{}", flake.store_path),
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

impl AnonymousJob {
    async fn resolve<RW>(
        self,
        nix_client: &mut NixClient<RW>,
    ) -> Result<(common::AnonymousJob, HashSet<wire::StorePath>), Error>
    where
        RW: AsyncRead + AsyncWrite + Unpin + Send,
    {
        match self {
            AnonymousJob::EvalJob(eval_job) => {
                let (eval_job, closure) = eval_job.resolve(nix_client).await?;
                Ok((
                    common::AnonymousJob {
                        r#type: Some(common::anonymous_job::Type::Eval(eval_job)),
                    },
                    closure,
                ))
            }
            AnonymousJob::BuildJob(build_job) => {
                let closure = todo!();
                Ok((
                    common::AnonymousJob {
                        r#type: Some(common::anonymous_job::Type::Build(build_job.into())),
                    },
                    closure,
                ))
            }
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

impl Client<Channel, UnixStream> {
    pub async fn new<S>(address: S) -> Result<Self, Error>
    where
        S: AsRef<str>,
    {
        let unix_stream = NixClient::connect_unix_default().await.unwrap();
        let nix_client = NixClient::handshake(unix_stream).await.unwrap();

        let tonic_client_client = PubSubClientClient::connect(address.as_ref().to_owned())
            .await
            .map_err(Into::<GrpcError>::into)?;

        let nar_copy_client = pubsub_nar_copy::Client::new(address).await.unwrap();

        Ok(Client {
            tonic_client_client,
            nar_copy_client,
            nix_client: Arc::new(RwLock::new(nix_client)),
        })
    }

    pub async fn submit_job(&mut self, anonymous_job: AnonymousJob) -> Result<Uuid, Error> {
        let mut nix_client = self.nix_client.write().await;
        let (eval_job, closure) = anonymous_job.resolve(&mut nix_client).await?;

        for store_path in closure {
            log::debug!("querying nar_info for {:?}", store_path);
            let nar_info = nix_client
                .query_path_info(&store_path)
                .await
                .unwrap()
                .unwrap();
            log::debug!("getting nar for {:?}", store_path);
            let nar = nix_client.nar_from_path(&store_path).await.unwrap();

            log::debug!("got nar_info {:?}", nar_info);

            self.nar_copy_client
                .upload_nar(wire::ValidPathInfo::new(store_path, nar_info), nar)
                .await
                .unwrap()
        }

        let response = self
            .tonic_client_client
            .submit_job(Request::new(client::SubmitJobRequest {
                job: Some(eval_job),
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
            .tonic_client_client
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
            .tonic_client_client
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
            .tonic_client_client
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
            .tonic_client_client
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
