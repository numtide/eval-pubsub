use pubsub_proto as proto;
use pubsub_proto::pub_sub_server::{PubSub, PubSubServer};

use anyhow::Result;
use tokio::task::JoinHandle;
use uuid::Uuid;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::queue::{Keyable, self as queue};

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

impl From<EvalJob> for proto::Job {
    fn from(value: EvalJob) -> Self {
        proto::Job {
            job_id: Some(value.job_id.into()),
            r#type: Some(proto::job::Type::Eval(proto::EvalJob {
                r#type: match value.r#type {
                    EvalJobType::Flake { flake_ref, attrs } => {
                        Some(proto::eval_job::Type::Flake(proto::eval_job::Flake {
                            flake_ref,
                            attr: attrs,
                        }))
                    }
                },
            })),
        }
    }
}

struct BuilderState {
    sender: queue::Sender<EvalJob, Uuid>,
    builder_information: proto::BuilderInformation,
    task: JoinHandle<Box<dyn Future<Output = ()> + Send>>,
}

pub struct PubSubService {
    pending_eval_jobs: Arc<RwLock<HashMap<Uuid, EvalJob>>>,
    picked_eval_jobs: Arc<RwLock<HashMap<Uuid, EvalJob>>>,

    builder_states: Arc<RwLock<HashMap<Uuid, BuilderState>>>,
}

#[tonic::async_trait]
impl PubSub for PubSubService {
    type ListJobsStream = ReceiverStream<Result<proto::ListJobsResponse, Status>>;
    async fn list_jobs(
        &self,
        request: Request<proto::ListJobsRequest>,
    ) -> Result<Response<Self::ListJobsStream>, Status> {
        match request.get_ref() {
            proto::ListJobsRequest { builder_information: Some(builder_information) } => {
                // TODO configurable?
                let (sender, mut receiver) = queue::new(256);
                let (tx, rx) = mpsc::channel(1);
                let pending_eval_jobs = self.pending_eval_jobs.clone();
                // TODO still racy
                self.builder_states.write().await.insert(builder_information.uuid.unwrap().into(), BuilderState {
                    sender,
                    builder_information: builder_information.clone(),
                    task: tokio::spawn(Box::pin(async move {
                        println!("background filter started");
                        for eval_job in pending_eval_jobs.read().await.values() {
                            tx.send(Ok(proto::ListJobsResponse {
                                job: Some(eval_job.clone().into())
                            })).await.unwrap();
                        }

                        loop {
                            match receiver.pop().await {
                                Some(eval_job) => {
                                    tx.send(Ok(proto::ListJobsResponse {
                                        job: Some(eval_job.clone().into())
                                    })).await.unwrap();
                                },
                                None => todo!(),
                            }
                        }
                    })),
                });

                Ok(Response::new(ReceiverStream::new(rx)))
            },
            _ => panic!("builder didn't provide BuilderInformation")
        }
    }

    async fn pick_job(
        &self,
        request: Request<proto::PickJobRequest>,
    ) -> Result<Response<proto::PickJobResponse>, Status> {
        match request.get_ref() {
            proto::PickJobRequest { job_id: Some(job_id) }  => {
                let job_id = (*job_id).into();
                println!("pending jobs: {:?}", self.pending_eval_jobs.read().await);
                match self.pending_eval_jobs.write().await.remove(&job_id) {
                    Some(eval_job) => {
                        self.picked_eval_jobs.write().await.insert(job_id, eval_job).ok_or(()).expect_err("cannot pick a already picked job, this shouldn't happen");

                        Ok(Response::new(proto::PickJobResponse{
                            pick_response: Some(proto::pick_job_response::PickResponse::Allow (proto::pick_job_response::Allow {}))
                        }))
                    },
                    None => Err(Status::not_found(format!("job with id {:?} not known or isn't pending", job_id)))
                }
            },
            _ => panic!("pick job request didn't come with an ID")
        }
    }

    async fn conclude_job(
        &self,
        _request: Request<proto::ConcludeJobRequest>,
    ) -> Result<Response<proto::ConcludeJobResponse>, Status> {
        unimplemented!();
    }

    async fn job_output(
        &self,
        _request: Request<proto::JobOutputRequest>,
    ) -> Result<Response<proto::JobOutputResponse>, Status> {
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

    pub async fn submit_eval_job(&mut self, eval_job: EvalJob) {
        println!("new job");
        self.pending_eval_jobs.write().await.insert(eval_job.job_id, eval_job.clone());

        for builder_state in self.builder_states.write().await.values_mut() {
            builder_state.sender.push(eval_job.clone()).await.ok_or(()).expect_err("failed to send eval job to a builder")
        }
    }
}
