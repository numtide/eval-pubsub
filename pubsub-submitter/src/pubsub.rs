use pubsub_proto::pubsub as proto;
use pubsub_proto::pubsub::server::pub_sub_server::PubSub;

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::error::{ProtocolError, SubmitterError};
use crate::queue::{self as queue, Keyable};

#[derive(Debug, Clone)]
pub enum EvalJobType {
    Flake {
        flake_ref: String,
        attrs: Vec<String>,
    },
}

#[derive(Debug, Clone)]
pub struct EvalJob {
    pub job_id: Uuid,
    pub r#type: EvalJobType,
}

impl Keyable for EvalJob {
    type Key = Uuid;

    fn key(&self) -> Self::Key {
        self.job_id
    }
}

impl From<EvalJob> for proto::server::Job {
    fn from(value: EvalJob) -> Self {
        proto::server::Job {
            job_id: Some(value.job_id.into()),
            r#type: Some(proto::server::job::Type::Eval(proto::common::EvalJob {
                r#type: match value.r#type {
                    EvalJobType::Flake { flake_ref, attrs } => Some(
                        proto::common::eval_job::Type::Flake(proto::common::eval_job::Flake {
                            flake_ref,
                            attr: attrs,
                        }),
                    ),
                },
            })),
        }
    }
}

struct BuilderState {
    sender: queue::Sender<EvalJob, Uuid>,
    builder_information: proto::server::BuilderInformation,
    task: JoinHandle<Box<dyn Future<Output = ()> + Send>>,
}

#[derive(Clone)]
pub struct PubSubService {
    pending_eval_jobs: Arc<RwLock<HashMap<Uuid, EvalJob>>>,
    picked_eval_jobs: Arc<RwLock<HashMap<Uuid, EvalJob>>>,

    builder_states: Arc<RwLock<HashMap<Uuid, BuilderState>>>,
}

#[tonic::async_trait]
impl PubSub for PubSubService {
    type ListJobsStream = ReceiverStream<Result<proto::server::ListJobsResponse, Status>>;
    async fn list_jobs(
        &self,
        request: Request<proto::server::ListJobsRequest>,
    ) -> Result<Response<Self::ListJobsStream>, Status> {
        let builder_information = request
            .get_ref()
            .builder_information
            .as_ref()
            .ok_or(ProtocolError::field_missing(&["builder_information"]))?;

        // TODO configurable?
        let (sender, mut receiver) = queue::new(256);
        let (tx, rx) = mpsc::channel(1);
        let pending_eval_jobs = self.pending_eval_jobs.clone();
        let builder_uuid = builder_information
            .uuid
            .ok_or(ProtocolError::field_missing(&[
                "builder_information",
                "uuid",
            ]))?
            .into();
        // TODO still racy
        self.builder_states.write().await.insert(
            builder_uuid,
            BuilderState {
                sender,
                builder_information: builder_information.clone(),
                task: tokio::spawn(Box::pin(async move {
                    println!("background filter started");
                    for eval_job in pending_eval_jobs.read().await.values() {
                        tx.send(Ok(proto::server::ListJobsResponse {
                            job: Some(eval_job.clone().into()),
                        }))
                        .await
                        .map_err(|_| SubmitterError::JobSend { builder_uuid })
                        .unwrap();
                    }

                    loop {
                        match receiver.pop().await {
                            Some(eval_job) => {
                                tx.send(Ok(proto::server::ListJobsResponse {
                                    job: Some(eval_job.clone().into()),
                                }))
                                .await
                                .map_err(|_| SubmitterError::JobSend { builder_uuid })
                                .unwrap();
                            }
                            None => todo!(),
                        }
                    }
                })),
            },
        );

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn pick_job(
        &self,
        request: Request<proto::server::PickJobRequest>,
    ) -> Result<Response<proto::server::PickJobResponse>, Status> {
        let job_id = request
            .get_ref()
            .job_id
            .as_ref()
            .ok_or(ProtocolError::field_missing(&["job_id"]))?
            .into();
        println!("pending jobs: {:?}", self.pending_eval_jobs.read().await);
        match self.pending_eval_jobs.write().await.remove(&job_id) {
            Some(eval_job) => {
                match self.picked_eval_jobs.write().await.insert(job_id, eval_job) {
                    Some(_) => Err(SubmitterError::JobPickedTwice { job_uuid: job_id }),
                    None => Ok(()),
                }?;

                Ok(Response::new(proto::server::PickJobResponse {
                    pick_response: Some(proto::server::pick_job_response::PickResponse::Allow(
                        proto::server::pick_job_response::Allow {},
                    )),
                }))
            }
            None => Err(SubmitterError::JobNotFound { job_uuid: job_id }.into()),
        }
    }

    async fn conclude_job(
        &self,
        _request: Request<proto::server::ConcludeJobRequest>,
    ) -> Result<Response<proto::server::ConcludeJobResponse>, Status> {
        unimplemented!();
    }

    async fn job_output(
        &self,
        _request: Request<proto::server::JobOutputRequest>,
    ) -> Result<Response<proto::server::JobOutputResponse>, Status> {
        unimplemented!();
    }
}

impl PubSubService {
    pub fn new() -> PubSubService {
        let pub_sub_service = PubSubService {
            pending_eval_jobs: Arc::new(RwLock::new(HashMap::new())),
            picked_eval_jobs: Arc::new(RwLock::new(HashMap::new())),

            builder_states: Arc::new(RwLock::new(HashMap::new())),
        };

        pub_sub_service
    }

    pub async fn submit_eval_job(&self, eval_job: EvalJob) -> Result<(), SubmitterError> {
        println!("new job");
        self.pending_eval_jobs
            .write()
            .await
            .insert(eval_job.job_id, eval_job.clone());

        for (builder_uuid, builder_state) in self.builder_states.write().await.iter_mut() {
            match builder_state
                .sender
                .push(eval_job.clone())
                .await {
                    Some(_) => Err(SubmitterError::JobSend { builder_uuid: *builder_uuid }),
                    None => Ok(()),
                }?
        }

        Ok(())
    }
}
