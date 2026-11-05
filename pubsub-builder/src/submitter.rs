use std::time::Duration;

use pubsub_proto::pubsub::{self as proto, server::pub_sub_client::PubSubClient};
use tokio::sync::mpsc;
use tonic::{
    Request, Status, Streaming,
    metadata::AsciiMetadataValue,
    service::{Interceptor, interceptor::InterceptedService},
    transport::{Channel, Endpoint},
};
use uuid::Uuid;

use crate::{error::BuilderError, executor::Conclusion};

const RETRY_DELAY: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct BuilderUuidInterceptor {
    builder_uuid: AsciiMetadataValue,
}

impl Interceptor for BuilderUuidInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        request
            .metadata_mut()
            .insert("x-builder-uuid", self.builder_uuid.clone());

        Ok(request)
    }
}

#[derive(Debug, Clone)]
pub struct Submitter {
    uri: String,
    client: PubSubClient<InterceptedService<Channel, BuilderUuidInterceptor>>,
}

#[derive(Debug, Clone)]
pub struct Offer {
    pub submitter: Submitter,
    pub job_id: Uuid,
    pub job: proto::common::AnonymousJob,
}

impl Submitter {
    pub fn connect_lazy(uri: String, builder_uuid: Uuid) -> Result<Self, BuilderError> {
        let channel = Endpoint::from_shared(uri.clone())?.connect_lazy();
        let interceptor = BuilderUuidInterceptor {
            builder_uuid: builder_uuid
                .to_string()
                .parse()
                .expect("a uuid is valid ascii metadata"),
        };

        Ok(Self {
            uri,
            client: PubSubClient::with_interceptor(channel, interceptor),
        })
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }

    pub async fn register(
        &self,
        builder_information: proto::common::BuilderInformation,
    ) -> Result<(), BuilderError> {
        self.client
            .clone()
            .register(proto::server::RegisterRequest {
                builder_information: Some(builder_information),
            })
            .await?;

        Ok(())
    }

    pub async fn list_jobs(
        &self,
    ) -> Result<Streaming<proto::server::ListJobsResponse>, BuilderError> {
        Ok(self
            .client
            .clone()
            .list_jobs(proto::server::ListJobsRequest {})
            .await?
            .into_inner())
    }

    pub async fn pick_job(&self, job_id: Uuid) -> Result<bool, BuilderError> {
        let response = self
            .client
            .clone()
            .pick_job(proto::server::PickJobRequest {
                job_id: Some(job_id.into()),
            })
            .await;

        match response.map(|response| response.into_inner().pick_response) {
            Ok(Some(proto::server::pick_job_response::PickResponse::Allow(_))) => Ok(true),
            Ok(Some(proto::server::pick_job_response::PickResponse::Deny(_))) => Ok(false),
            Ok(None) => Err(BuilderError::field_missing(&["pick_response"])),
            Err(status) if status.code() == tonic::Code::NotFound => Ok(false),
            Err(status) => Err(status.into()),
        }
    }

    pub async fn job_output(&self, job_id: Uuid, output: Vec<String>) -> Result<(), BuilderError> {
        self.client
            .clone()
            .job_output(proto::server::JobOutputRequest {
                job_id: Some(job_id.into()),
                output,
            })
            .await?;

        Ok(())
    }

    pub async fn conclude_job(
        &self,
        job_id: Uuid,
        conclusion: Conclusion,
    ) -> Result<(), BuilderError> {
        let (job_status, result) = conclusion.into_request_parts();

        self.client
            .clone()
            .conclude_job(proto::server::ConcludeJobRequest {
                job_id: Some(job_id.into()),
                job_status: Some(job_status),
                result,
            })
            .await?;

        Ok(())
    }
}

pub async fn run(
    submitter: Submitter,
    builder_information: proto::common::BuilderInformation,
    offers: mpsc::Sender<Offer>,
) {
    loop {
        match subscribe(&submitter, &builder_information, &offers).await {
            Ok(()) => return,
            Err(err) => log::warn!(
                "lost connection to submitter {}: {}, retrying in {:?}",
                submitter.uri(),
                err,
                RETRY_DELAY
            ),
        }

        tokio::time::sleep(RETRY_DELAY).await;
    }
}

async fn subscribe(
    submitter: &Submitter,
    builder_information: &proto::common::BuilderInformation,
    offers: &mpsc::Sender<Offer>,
) -> Result<(), BuilderError> {
    submitter.register(builder_information.clone()).await?;
    let mut stream = submitter.list_jobs().await?;

    log::info!("subscribed to submitter {}", submitter.uri());

    while let Some(response) = stream.message().await? {
        match offer_from(submitter, response) {
            Ok(offer) => {
                if offers.send(offer).await.is_err() {
                    return Ok(());
                }
            }
            Err(err) => log::warn!(
                "ignoring malformed job from submitter {}: {}",
                submitter.uri(),
                err
            ),
        }
    }

    Err(BuilderError::StreamClosed)
}

fn offer_from(
    submitter: &Submitter,
    response: proto::server::ListJobsResponse,
) -> Result<Offer, BuilderError> {
    let job = response
        .job
        .ok_or_else(|| BuilderError::field_missing(&["job"]))?;

    Ok(Offer {
        submitter: submitter.clone(),
        job_id: job
            .job_id
            .ok_or_else(|| BuilderError::field_missing(&["job", "job_id"]))?
            .into(),
        job: job
            .anonymous_job
            .ok_or_else(|| BuilderError::field_missing(&["job", "anonymous_job"]))?,
    })
}
