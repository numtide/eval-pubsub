use pubsub_proto as proto;
use pubsub_proto::pub_sub_server::{PubSub, PubSubServer};

use anyhow::Result;
use uuid::Uuid;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

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

#[derive(Debug)]
pub struct PubSubService {
    pending_eval_jobs: Arc<RwLock<HashMap<Uuid, EvalJob>>>,
    picked_eval_jobs: Arc<RwLock<HashMap<Uuid, EvalJob>>>,
    eval_jobs_chan: (broadcast::Sender<EvalJob>, broadcast::Receiver<EvalJob>),
    builders: Arc<RwLock<HashMap<Uuid, proto::BuilderInformation>>>,
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
                self.builders.write().await.insert(builder_information.uuid.unwrap().into(), builder_information.clone());

                let (tx, rx) = mpsc::channel(4);
                let mut eval_jobs = self.eval_jobs_chan.0.subscribe();

                // TODO this thing here is most definitely racy
                {
                    let tx = tx.clone();
                    let pending_eval_jobs = self.pending_eval_jobs.read().await.clone();
                    tokio::spawn(async move {
                        for eval_job in pending_eval_jobs.values() {
                            println!("returning pending job {:?}", eval_job);
                            let _ = tx.send(Ok(proto::ListJobsResponse {
                                job: Some(eval_job.clone().into())
                            })).await;
                        }
                    });
                };
                {
                    let builder_uuid = builder_information.uuid.unwrap().into();
                    let builders = self.builders.clone();
                    tokio::spawn(async move {
                        while let Ok(eval_job) = eval_jobs.recv().await {
                            match tx.send(Ok(proto::ListJobsResponse {
                                job: Some(eval_job.into())
                            })).await {
                                Ok(_) => { /* all good */ },
                                Err(_) => {
                                    // closed
                                    builders.write().await.remove(&builder_uuid);
                                },
                            };
                        }
                    });
                }

                Ok(Response::new(ReceiverStream::new(rx)))
            },
            _ => panic!("builder didn't provide BuilderInformation")
        }
    }

    async fn pick_job(
        &self,
        _request: Request<proto::PickJobRequest>,
    ) -> Result<Response<proto::PickJobResponse>, Status> {
        unimplemented!();
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
        let chan = broadcast::channel(128);

        let route_guide = PubSubService {
            eval_jobs_chan: chan,
            builders: Arc::new(RwLock::new(HashMap::new())),
            pending_eval_jobs: Arc::new(RwLock::new(HashMap::new())),
            picked_eval_jobs: Arc::new(RwLock::new(HashMap::new())),
        };

        route_guide
    }

    pub async fn submit_eval_job(&mut self, eval_job: EvalJob) -> Result<usize> {
        println!("new job");

        let uuid = Uuid::new_v4();

        let size = self.eval_jobs_chan.0.send(eval_job.clone())?;
        self.pending_eval_jobs.write().await.insert(uuid, eval_job);
        Ok(size)
    }
}
