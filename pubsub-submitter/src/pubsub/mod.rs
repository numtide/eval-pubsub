use diesel::query_dsl::methods::{FilterDsl as _, SelectDsl as _};
use diesel::sql_types::Text;
use diesel::{ExpressionMethods as _, OptionalExtension as _, SelectableHelper as _, sql_query};
use diesel_async::pooled_connection::deadpool::Object;
use diesel_async::{AsyncConnection as _, AsyncPgConnection, RunQueryDsl as _};
use pubsub_proto::pubsub as proto;
use pubsub_proto::pubsub::client::pub_sub_client_server::PubSubClient;
use pubsub_proto::pubsub::server::pub_sub_server::PubSub;
use tokio_stream::StreamExt as _;

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Notify, RwLock, SetOnce, mpsc};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::error::{ProtocolError, SubmitterError};
use crate::pubsub::concluded_job_notification::ConcludedJobNotification;
use crate::pubsub::db_connector::DoContinue;
use crate::pubsub::job_output_notification::JobOutputNotification;
use crate::queue::{self as queue, Keyable};
use crate::schema::builder;
use crate::{error::ThroughProtocolError, models};
use db_connector::DBConnector;

mod concluded_job_notification;
mod db_connector;
mod job_output_notification;

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

impl From<EvalJob> for proto::common::Job {
    fn from(value: EvalJob) -> Self {
        proto::common::Job {
            job_id: Some(value.job_id.into()),
            anonymous_job: Some(value.r#type.into()),
        }
    }
}

impl From<EvalJobType> for proto::common::AnonymousJob {
    fn from(value: EvalJobType) -> Self {
        proto::common::AnonymousJob {
            r#type: Some(proto::common::anonymous_job::Type::Eval(
                proto::common::EvalJob {
                    r#type: Some(value.into()),
                },
            )),
        }
    }
}

impl From<EvalJobType> for proto::common::EvalJob {
    fn from(value: EvalJobType) -> Self {
        proto::common::EvalJob {
            r#type: Some(value.into()),
        }
    }
}

impl From<EvalJobType> for proto::common::eval_job::Type {
    fn from(value: EvalJobType) -> Self {
        match value {
            EvalJobType::Flake { flake_ref, attrs } => {
                proto::common::eval_job::Type::Flake(proto::common::eval_job::Flake {
                    flake_ref,
                    attr: attrs,
                })
            }
        }
    }
}

impl TryFrom<proto::common::EvalJob> for EvalJob {
    type Error = ProtocolError;

    fn try_from(value: proto::common::EvalJob) -> Result<Self, Self::Error> {
        match value.r#type {
            Some(r#type) => {
                let uuid = Uuid::new_v4();

                Ok(EvalJob {
                    job_id: uuid,
                    r#type: match r#type {
                        proto::common::eval_job::Type::Flake(flake) => EvalJobType::Flake {
                            flake_ref: flake.flake_ref,
                            attrs: flake.attr,
                        },
                    },
                })
            }
            None => Err(ProtocolError::field_missing(&["type"])),
        }
    }
}

struct BuilderTailer {
    sender: queue::Sender<EvalJob, Uuid>,
    task: JoinHandle<()>,
}

#[derive(Clone)]
pub struct PubSubService {
    db_connector: DBConnector,
    builder_states: Arc<RwLock<HashMap<Uuid, BuilderTailer>>>,
    shutdown_cell: Arc<SetOnce<()>>,
}

impl PubSubService {
    async fn check_registered(
        &self,
        conn: &mut Object<AsyncPgConnection>,
        builder_uuid: Uuid,
    ) -> Result<(), SubmitterError> {
        let builder: Option<models::Builder> = builder::dsl::builder
            .filter(builder::dsl::id.eq(builder_uuid))
            .select(models::Builder::as_select())
            .first(conn)
            .await
            .optional()?;

        match builder {
            Some(_builder) => Ok(()),
            None => Err(SubmitterError::Unregistered {}),
        }
    }
}

fn get_uuid_from_request<T>(request: &Request<T>) -> Result<Uuid, SubmitterError> {
    request
        .metadata()
        .get("x-builder-uuid")
        .ok_or(ProtocolError::metadata_missing("uuid"))
        .and_then(|metadata_value| {
            metadata_value
                .to_str()
                .map_err(|_| ProtocolError::metadata_invalid("uuid"))
        })
        .and_then(|metadata_value| {
            Uuid::parse_str(metadata_value).map_err(|_| ProtocolError::metadata_invalid("uuid"))
        })
        .map_err(Into::into)
}

pub fn spawn_with_shutdown<F1, F2>(
    shutdown: Arc<SetOnce<()>>,
    future: F1,
    finally: F2,
) -> JoinHandle<F2::Output>
where
    F1: Future<Output = ()> + Send + 'static,
    F2: Future + Send + 'static,
    F2::Output: Send + 'static,
{
    tokio::spawn(async move {
        tokio::select!(
            _ = future => {},
            _ = shutdown.wait() => {}
        );

        finally.await
    })
}

#[tonic::async_trait]
impl PubSub for PubSubService {
    type ListJobsStream = ReceiverStream<Result<proto::server::ListJobsResponse, Status>>;

    async fn register(
        &self,
        request: Request<proto::server::RegisterRequest>,
    ) -> Result<Response<proto::server::RegisterResponse>, Status> {
        let mut conn = self.db_connector.get().await?;
        let builder_information = request
            .get_ref()
            .builder_information
            .as_ref()
            .ok_or(ProtocolError::field_missing(&["builder_information"]))?;
        let builder_uuid = builder_information
            .uuid
            .ok_or(ProtocolError::field_missing(&[
                "builder_information",
                "uuid",
            ]))?
            .into();

        println!(
            "builder {} registered: {:?}",
            builder_uuid, builder_information
        );

        db_connector::insert_builder(
            &mut conn,
            builder_uuid,
            &builder_information.system,
            builder_information.smt_core_count,
            builder_information.total_memory,
            false,
        )
        .await?;

        Ok(Response::new(proto::server::RegisterResponse {}))
    }

    async fn list_jobs(
        &self,
        request: Request<proto::server::ListJobsRequest>,
    ) -> Result<Response<Self::ListJobsStream>, Status> {
        let mut conn = self.db_connector.get().await.through()?;
        let builder_uuid = get_uuid_from_request(&request)?;
        self.check_registered(&mut conn, builder_uuid).await?;
        let streaming = Arc::new(Notify::new());

        // TODO configurable?
        let (sender, mut receiver) = queue::new::<EvalJob, Uuid>(256);
        let (tx, rx) = mpsc::channel(1);

        db_connector::set_builder_active(&mut conn, builder_uuid, true).await?;

        let task = {
            let shutdown_cell = self.shutdown_cell.clone();
            let mut conn = self.db_connector.get().await.through()?;
            let tx = tx.clone();
            let streaming = streaming.clone();
            spawn_with_shutdown(
                shutdown_cell,
                async move {
                    streaming.notify_one();
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
                            // no more jobs, shutting down
                            None => {}
                        }
                    }
                },
                async move {
                    db_connector::set_builder_active(&mut conn, builder_uuid, false)
                        .await
                        .unwrap();
                },
            )
        };

        self.builder_states
            .write()
            .await
            .insert(builder_uuid, BuilderTailer { sender, task });

        streaming.notified().await;

        let pending_eval_jobs = db_connector::get_pending_jobs(&mut conn).await?;

        for eval_job in pending_eval_jobs {
            tx.send(Ok(proto::server::ListJobsResponse {
                job: Some(eval_job.clone().into()),
            }))
            .await
            .map_err(|_| SubmitterError::JobSend { builder_uuid })
            .unwrap();
        }

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn pick_job(
        &self,
        request: Request<proto::server::PickJobRequest>,
    ) -> Result<Response<proto::server::PickJobResponse>, Status> {
        let mut conn = self.db_connector.get().await.through()?;
        let builder_uuid = get_uuid_from_request(&request)?;
        self.check_registered(&mut conn, builder_uuid).await?;

        let job_id = request
            .get_ref()
            .job_id
            .as_ref()
            .ok_or(ProtocolError::field_missing(&["job_id"]))?
            .into();
        conn.transaction::<_, SubmitterError, _>(async |mut conn| {
            let pending_job = db_connector::take_pending_job(&mut conn, job_id).await?;

            match pending_job {
                Some(eval_job) => {
                    db_connector::insert_picked_jobs(
                        conn,
                        &vec![models::PickedEvalJob {
                            id: eval_job.id,
                            builder_id: builder_uuid,
                        }],
                    )
                    .await?;

                    Ok(Response::new(proto::server::PickJobResponse {
                        pick_response: Some(proto::server::pick_job_response::PickResponse::Allow(
                            proto::server::pick_job_response::Allow {},
                        )),
                    }))
                }
                None => {
                    let eval_job: Option<models::PickedEvalJob> =
                        db_connector::get_picked_job(&mut conn, job_id).await?;

                    match eval_job {
                        Some(_) => Ok(Response::new(proto::server::PickJobResponse {
                            pick_response: Some(
                                proto::server::pick_job_response::PickResponse::Deny(
                                    proto::server::pick_job_response::Deny {},
                                ),
                            ),
                        })),
                        None => Err(SubmitterError::JobNotFound { job_uuid: job_id }.into()),
                    }
                }
            }
        })
        .await
        .map_err(Into::into)
    }

    async fn conclude_job(
        &self,
        request: Request<proto::server::ConcludeJobRequest>,
    ) -> Result<Response<proto::server::ConcludeJobResponse>, Status> {
        let mut conn = self.db_connector.get().await.through()?;
        let builder_uuid = get_uuid_from_request(&request)?;
        self.check_registered(&mut conn, builder_uuid).await?;

        let job_id = request
            .get_ref()
            .job_id
            .as_ref()
            .ok_or(ProtocolError::field_missing(&["job_id"]))?
            .into();

        conn.transaction::<_, SubmitterError, _>(async |mut conn| {
            match db_connector::take_picked_job(&mut conn, job_id).await? {
                Some(eval_job) => {
                    db_connector::insert_concluded_jobs(
                        &mut conn,
                        &vec![models::ConcludedEvalJob {
                            id: eval_job.id,
                            builder_id: builder_uuid,
                        }],
                    )
                    .await?;
                    db_connector::notify(
                        &mut conn,
                        "concluded_job",
                        ConcludedJobNotification::new(job_id).to_json()?,
                    )
                    .await?;

                    Ok(Response::new(proto::server::ConcludeJobResponse {}))
                }
                None => Err(SubmitterError::JobNotFound { job_uuid: job_id }.into()),
            }
        })
        .await
        .map_err(Into::into)
    }

    async fn job_output(
        &self,
        request: Request<proto::server::JobOutputRequest>,
    ) -> Result<Response<proto::server::JobOutputResponse>, Status> {
        let job_uuid: Uuid = request
            .get_ref()
            .job_id
            .as_ref()
            .ok_or(ProtocolError::field_missing(&["job_id"]))?
            .into();
        let output: &Vec<String> = &request.get_ref().output;

        let mut conn = self.db_connector.get().await.through()?;
        conn.transaction::<_, SubmitterError, _>(async |mut conn| {
            let uuids = db_connector::insert_job_outputs(
                &mut conn,
                output
                    .into_iter()
                    .map(|line| models::NewJobOutput {
                        job_id: job_uuid,
                        line,
                    })
                    .collect::<Vec<_>>(),
            )
            .await?;
            for job_output_id in uuids {
                let job_output_notification = JobOutputNotification::new(job_uuid, job_output_id);
                println!("notifying \"job_output\" for {}", job_output_id);

                db_connector::notify(&mut conn, "job_output", job_output_notification.to_json()?)
                    .await?;
            }

            println!("got job {job_uuid} output:");
            for line in output {
                println!("  {line}");
            }

            Ok(())
        })
        .await
        .map_err(Into::<SubmitterError>::into)?;

        Ok(Response::new(proto::server::JobOutputResponse {}))
    }
}

#[tonic::async_trait]
impl PubSubClient for PubSubService {
    type TailJobStream = ReceiverStream<Result<proto::client::TailJobResponse, Status>>;

    async fn submit_job(
        &self,
        request: Request<proto::client::SubmitJobRequest>,
    ) -> Result<Response<proto::client::SubmitJobResponse>, Status> {
        match &request.get_ref().r#type {
            Some(proto::client::submit_job_request::Type::Eval(eval_job)) => {
                let eval_job: EvalJob = eval_job.clone().try_into()?;
                let uuid = eval_job.job_id.into();
                self.submit_eval_job(eval_job).await?;

                Ok(Response::new(proto::client::SubmitJobResponse {
                    job_id: Some(uuid),
                }))
            }
            Some(proto::client::submit_job_request::Type::Build(_build_job)) => todo!(),
            None => todo!(),
        }
    }

    async fn tail_job(
        &self,
        request: Request<proto::client::TailJobRequest>,
    ) -> Result<Response<Self::TailJobStream>, Status> {
        let mut conn = self.db_connector.get().await?;
        let mut conn_notifs = self.db_connector.get().await?;
        let job_id: Uuid = request
            .get_ref()
            .job_id
            .as_ref()
            .ok_or(ProtocolError::field_missing(&["job_id"]))?
            .into();

        let (tx, rx) = mpsc::channel(1);

        {
            let shutdown_cell = self.shutdown_cell.clone();
            spawn_with_shutdown(
                shutdown_cell,
                async move {
                    db_connector::listen(
                        &mut conn_notifs,
                        "job_output",
                        async move |job_output_notification: JobOutputNotification| {
                            if job_output_notification.job_uuid == job_id {
                                match db_connector::get_job_output(
                                    &mut conn,
                                    job_output_notification.job_output_id,
                                )
                                .await
                                .unwrap()
                                {
                                    Some(job_output) => {
                                        println!("sending job output");
                                        tx.send(Ok(proto::client::TailJobResponse {
                                            line: job_output.line,
                                        }))
                                        .await
                                        .unwrap();
                                        println!("sent job output");
                                    }
                                    None => {
                                        tx.send(Err(SubmitterError::JobOutputNotFound {
                                            job_output_id: job_output_notification.job_output_id,
                                        }
                                        .into()))
                                            .await
                                            .unwrap();
                                    }
                                }
                            }

                            println!("getting next");
                            DoContinue::Continue
                        },
                    )
                    .await
                    .unwrap()
                },
                async move {},
            );
        }

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn wait_job(
        &self,
        request: Request<proto::client::WaitJobRequest>,
    ) -> Result<Response<proto::client::WaitJobResponse>, Status> {
        let job_id: Uuid = request
            .get_ref()
            .job_id
            .as_ref()
            .ok_or(ProtocolError::field_missing(&["job_id"]))?
            .into();
        let result = Arc::new(SetOnce::new());
        let mut conn_notifs = self.db_connector.get().await?;

        {
            let result = result.clone();
            db_connector::listen(
                &mut conn_notifs,
                "concluded_job",
                async move |concluded_job_notification: ConcludedJobNotification| {
                    println!("job output notification: {:?}", concluded_job_notification);
                    if concluded_job_notification.job_uuid == job_id {
                        println!("matched");
                        result
                            .set(proto::client::WaitJobResponse {
                                job_status: Some(proto::common::JobStatus {
                                    result: Some(proto::common::job_status::Result::Success(
                                        proto::common::job_status::Success {},
                                    )),
                                }),
                            })
                            .unwrap();
                        DoContinue::Stop(Ok(()))
                    } else {
                        DoContinue::Continue
                    }
                },
            )
            .await?;
        }

        Ok(Response::new(*result.get().unwrap()))
    }

    async fn list_jobs(
        &self,
        request: Request<proto::client::ListJobsRequest>,
    ) -> Result<Response<proto::client::ListJobsResponse>, Status> {
        Err(Status::unimplemented("todo"))
    }

    async fn list_builders(
        &self,
        request: Request<proto::client::ListBuildersRequest>,
    ) -> Result<Response<proto::client::ListBuildersResponse>, Status> {
        Err(Status::unimplemented("todo"))
    }
}

impl PubSubService {
    pub async fn wait_shutdown(self) {
        println!("collecting builder tailers");
        self.shutdown_cell.set(()).unwrap();
        for (uuid, builder_tailer) in self.builder_states.write().await.drain() {
            println!("tailer done {}", uuid);
            builder_tailer.task.await.unwrap()
        }
    }

    pub fn new<S>(database_url: S) -> Result<PubSubService, Box<dyn std::error::Error>>
    where
        S: AsRef<str>,
    {
        let pub_sub_service = PubSubService {
            builder_states: Arc::new(RwLock::new(HashMap::new())),
            db_connector: DBConnector::new(database_url)?,
            shutdown_cell: Arc::new(SetOnce::new()),
        };

        Ok(pub_sub_service)
    }

    pub fn run_migrations(
        &mut self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        self.db_connector.run_migrations()
    }

    pub async fn clear_db(&mut self) {
        let mut conn = self.db_connector.get().await.through().unwrap();

        diesel::sql_query("DROP SCHEMA public CASCADE;")
            .execute(&mut conn)
            .await
            .unwrap();
        diesel::sql_query("CREATE SCHEMA public;")
            .execute(&mut conn)
            .await
            .unwrap();
    }

    pub async fn submit_eval_job(&self, eval_job: EvalJob) -> Result<(), SubmitterError> {
        println!("new job");
        let mut conn = self.db_connector.get().await.through()?;
        {
            let eval_job = eval_job.clone();

            conn.transaction::<_, SubmitterError, _>(async |mut conn| {
                let job_id = eval_job.job_id;
                db_connector::insert_eval_jobs(
                    &mut conn,
                    &vec![Into::<models::EvalJob>::into(eval_job)],
                )
                .await?;

                db_connector::insert_pending_jobs(
                    &mut conn,
                    &vec![models::PendingEvalJob { id: job_id }],
                )
                .await?;

                Ok(())
            })
            .await
            .map_err(Into::<SubmitterError>::into)?;
        }

        for (builder_uuid, builder_tailer) in self.builder_states.write().await.iter_mut() {
            match builder_tailer.sender.push(eval_job.clone()).await {
                Some(_) => Err(SubmitterError::JobSend {
                    builder_uuid: *builder_uuid,
                }),
                None => Ok(()),
            }?
        }

        Ok(())
    }
}
