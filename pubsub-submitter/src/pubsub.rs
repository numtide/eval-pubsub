use diesel::query_dsl::methods::{FilterDsl as _, SelectDsl as _};
use diesel::{
    Connection as _, ExpressionMethods as _, OptionalExtension as _, PgConnection,
    SelectableHelper as _,
};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::deadpool::{Object, Pool};
use diesel_async::{AsyncConnection as _, AsyncPgConnection, RunQueryDsl as _};
use diesel_migrations::MigrationHarness as _;
use pubsub_proto::pubsub as proto;
use pubsub_proto::pubsub::client::pub_sub_client_server::PubSubClient;
use pubsub_proto::pubsub::server::pub_sub_server::PubSub;

use std::cell::OnceCell;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, SetOnce, mpsc};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::error::{ProtocolError, SubmitterError};
use crate::queue::{self as queue, Keyable};
use crate::schema::builder;
use crate::schema::eval_job;
use crate::schema::pending_eval_job;
use crate::schema::picked_eval_job;
use crate::schema::{concluded_eval_job, job_output};
use crate::{error::ThroughProtocolError, models, schema};

pub const MIGRATIONS: diesel_migrations::EmbeddedMigrations =
    diesel_migrations::embed_migrations!("migrations");

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
    pg_pool: Pool<AsyncPgConnection>,
    database_url: String,

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

#[tonic::async_trait]
impl PubSub for PubSubService {
    type ListJobsStream = ReceiverStream<Result<proto::server::ListJobsResponse, Status>>;

    async fn register(
        &self,
        request: Request<proto::server::RegisterRequest>,
    ) -> Result<Response<proto::server::RegisterResponse>, Status> {
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

        let mut conn = self.pg_pool.get().await.through()?;
        conn.transaction::<_, SubmitterError, _>(async |mut conn| {
            diesel::insert_into(builder::dsl::builder)
                .values(&vec![models::Builder {
                    id: builder_uuid,
                    system: builder_information.system.clone(),
                    smt_core_count: builder_information.smt_core_count,
                    // TODO down sizing this value
                    total_memory: builder_information.total_memory as i64,
                    active: false,
                }])
                .execute(&mut conn)
                .await
                .expect("Failed to insert row");

            Ok(Response::new(proto::server::RegisterResponse {}))
        })
        .await
        .map_err(Into::into)
    }

    async fn list_jobs(
        &self,
        request: Request<proto::server::ListJobsRequest>,
    ) -> Result<Response<Self::ListJobsStream>, Status> {
        let mut conn = self.pg_pool.get().await.through()?;
        let builder_uuid = get_uuid_from_request(&request)?;
        self.check_registered(&mut conn, builder_uuid).await?;

        // TODO configurable?
        let (sender, mut receiver) = queue::new(256);
        let (tx, rx) = mpsc::channel(1);

        let pending_eval_jobs: Vec<EvalJob> = eval_job::dsl::eval_job
            .select(models::EvalJob::as_select())
            .load(&mut conn)
            .await
            .through()?
            .into_iter()
            .map(TryInto::<EvalJob>::try_into)
            .collect::<Result<Vec<EvalJob>, _>>()
            .map_err(Into::<SubmitterError>::into)?;

        assert_eq!(
            1,
            diesel::update(builder::dsl::builder.filter(builder::dsl::id.eq(builder_uuid)))
                .set(builder::dsl::active.eq(true))
                .execute(&mut conn)
                .await
                .map_err(Into::<SubmitterError>::into)?
        );

        let shutdown_cell = self.shutdown_cell.clone();
        // TODO still racy
        self.builder_states.write().await.insert(
            builder_uuid,
            BuilderTailer {
                sender,
                task: tokio::spawn(async move {
                    println!("background filter started");
                    for eval_job in pending_eval_jobs {
                        tx.send(Ok(proto::server::ListJobsResponse {
                            job: Some(eval_job.clone().into()),
                        }))
                        .await
                        .map_err(|_| SubmitterError::JobSend { builder_uuid })
                        .unwrap();
                    }

                    tokio::select! {
                        _ = async move {
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
                        } => {},
                        _ = shutdown_cell.wait() => {}
                    };

                    assert_eq!(
                        1,
                        diesel::update(
                            builder::dsl::builder.filter(builder::dsl::id.eq(builder_uuid))
                        )
                        .set(builder::dsl::active.eq(false))
                        .execute(&mut conn)
                        .await
                        .map_err(Into::<SubmitterError>::into)
                        .expect("failed to update builder active state")
                    );
                }),
            },
        );

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn pick_job(
        &self,
        request: Request<proto::server::PickJobRequest>,
    ) -> Result<Response<proto::server::PickJobResponse>, Status> {
        let mut conn = self.pg_pool.get().await.through()?;
        let builder_uuid = get_uuid_from_request(&request)?;
        self.check_registered(&mut conn, builder_uuid).await?;

        let job_id = request
            .get_ref()
            .job_id
            .as_ref()
            .ok_or(ProtocolError::field_missing(&["job_id"]))?
            .into();
        conn.transaction::<_, SubmitterError, _>(async |mut conn| {
            let pending_job: Option<models::PendingEvalJob> = diesel::delete(
                pending_eval_job::dsl::pending_eval_job
                    .filter(pending_eval_job::dsl::id.eq(job_id)),
            )
            .get_result::<models::PendingEvalJob>(&mut conn)
            .await
            .optional()?;

            match pending_job {
                Some(eval_job) => {
                    diesel::insert_into(picked_eval_job::dsl::picked_eval_job)
                        .values(&vec![models::PickedEvalJob {
                            id: eval_job.id,
                            builder_id: builder_uuid,
                        }])
                        .execute(&mut conn)
                        .await
                        .expect("Failed to insert row");

                    Ok(Response::new(proto::server::PickJobResponse {
                        pick_response: Some(proto::server::pick_job_response::PickResponse::Allow(
                            proto::server::pick_job_response::Allow {},
                        )),
                    }))
                }
                None => {
                    let eval_job: Option<models::PickedEvalJob> =
                        picked_eval_job::dsl::picked_eval_job
                            .filter(picked_eval_job::dsl::id.eq(job_id))
                            .select(models::PickedEvalJob::as_select())
                            .first(&mut conn)
                            .await
                            .optional()?;

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
        let mut conn = self.pg_pool.get().await.through()?;
        let builder_uuid = get_uuid_from_request(&request)?;
        self.check_registered(&mut conn, builder_uuid).await?;

        let job_id = request
            .get_ref()
            .job_id
            .as_ref()
            .ok_or(ProtocolError::field_missing(&["job_id"]))?
            .into();

        conn.transaction::<_, SubmitterError, _>(async |mut conn| {
            let picked_job: Option<models::PickedEvalJob> = diesel::delete(
                picked_eval_job::dsl::picked_eval_job.filter(picked_eval_job::dsl::id.eq(job_id)),
            )
            .get_result::<models::PickedEvalJob>(&mut conn)
            .await
            .optional()?;

            match picked_job {
                Some(eval_job) => {
                    diesel::insert_into(concluded_eval_job::dsl::concluded_eval_job)
                        .values(&vec![models::ConcludedEvalJob {
                            id: eval_job.id,
                            builder_id: builder_uuid,
                        }])
                        .execute(&mut conn)
                        .await
                        .expect("Failed to insert row");

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
        let job_id: Uuid = request
            .get_ref()
            .job_id
            .as_ref()
            .ok_or(ProtocolError::field_missing(&["job_id"]))?
            .into();
        let output: &Vec<String> = &request.get_ref().output;

        let mut conn = self.pg_pool.get().await.through()?;
        conn.transaction::<_, SubmitterError, _>(async |mut conn| {
            diesel::insert_into(job_output::dsl::job_output)
                .values(
                    output
                        .into_iter()
                        .map(|line| models::NewJobOutput { job_id, line })
                        .collect::<Vec<_>>(),
                )
                .execute(&mut conn)
                .await?;

            println!("got job {job_id} output:");
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
            Some(proto::client::submit_job_request::Type::Build(build_job)) => todo!(),
            None => todo!(),
        }
    }

    async fn tail_job(
        &self,
        request: Request<proto::client::TailJobRequest>,
    ) -> Result<Response<Self::TailJobStream>, Status> {
        Err(Status::unimplemented("todo"))
    }

    async fn wait_job(
        &self,
        request: Request<proto::client::WaitJobRequest>,
    ) -> Result<Response<proto::client::WaitJobResponse>, Status> {
        Err(Status::unimplemented("todo"))
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
        let config = AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(
            database_url.as_ref(),
        );
        let pool = Pool::builder(config).build()?;
        let pub_sub_service = PubSubService {
            builder_states: Arc::new(RwLock::new(HashMap::new())),
            pg_pool: pool,
            database_url: database_url.as_ref().to_owned(),
            shutdown_cell: Arc::new(SetOnce::new()),
        };

        Ok(pub_sub_service)
    }

    pub fn run_migrations(
        &mut self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        let mut conn = PgConnection::establish(&self.database_url)?;
        conn.run_pending_migrations(MIGRATIONS)?;

        Ok(())
    }

    pub async fn clear_db(&mut self) {
        let mut conn = self.pg_pool.get().await.through().unwrap();

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
        let mut conn = self.pg_pool.get().await.through()?;
        {
            let eval_job = eval_job.clone();

            conn.transaction::<_, SubmitterError, _>(async |mut conn| {
                let job_id = eval_job.job_id;
                diesel::insert_into(eval_job::dsl::eval_job)
                    .values(&vec![Into::<models::EvalJob>::into(eval_job)])
                    .execute(&mut conn)
                    .await?;

                diesel::insert_into(pending_eval_job::dsl::pending_eval_job)
                    .values(&vec![models::PendingEvalJob { id: job_id }])
                    .execute(&mut conn)
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
