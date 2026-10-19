use diesel::query_dsl::methods::{FilterDsl as _, SelectDsl as _};
use diesel::{
    Connection as _, ExpressionMethods as _, OptionalExtension as _, PgConnection,
    SelectableHelper as _,
};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::deadpool::Pool;
use diesel_async::{AsyncConnection as _, AsyncPgConnection, RunQueryDsl as _};
use diesel_migrations::MigrationHarness as _;
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
use crate::schema::concluded_eval_job::dsl as concluded_eval_job_dsl;
use crate::schema::eval_job::dsl as eval_job_dsl;
use crate::schema::pending_eval_job::dsl as pending_eval_job_dsl;
use crate::schema::picked_eval_job::dsl as picked_eval_job_dsl;
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
    pg_pool: Pool<AsyncPgConnection>,
    database_url: String,

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

        let mut conn = self.pg_pool.get().await.through()?;
        let pending_eval_jobs: Vec<EvalJob> = eval_job_dsl::eval_job
            .select(models::EvalJob::as_select())
            .load(&mut conn)
            .await
            .through()?
            .into_iter()
            .map(TryInto::<EvalJob>::try_into)
            .collect::<Result<Vec<EvalJob>, _>>()
            .map_err(Into::<SubmitterError>::into)?;

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
                    for eval_job in pending_eval_jobs {
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

        let mut conn = self.pg_pool.get().await.through()?;
        conn.transaction::<_, SubmitterError, _>(async |mut conn| {
            let pending_job: Option<models::PendingEvalJob> = diesel::delete(
                pending_eval_job_dsl::pending_eval_job.filter(pending_eval_job_dsl::id.eq(job_id)),
            )
            .get_result::<models::PendingEvalJob>(&mut conn)
            .await
            .optional()?;

            match pending_job {
                Some(eval_job) => {
                    diesel::insert_into(picked_eval_job_dsl::picked_eval_job)
                        .values(&vec![models::PickedEvalJob { id: eval_job.id }])
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
                        picked_eval_job_dsl::picked_eval_job
                            .filter(picked_eval_job_dsl::id.eq(job_id))
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
        let job_id = request
            .get_ref()
            .job_id
            .as_ref()
            .ok_or(ProtocolError::field_missing(&["job_id"]))?
            .into();

        let mut conn = self.pg_pool.get().await.through()?;
        conn.transaction::<_, SubmitterError, _>(async |mut conn| {
            let picked_job: Option<models::PickedEvalJob> = diesel::delete(
                picked_eval_job_dsl::picked_eval_job.filter(picked_eval_job_dsl::id.eq(job_id)),
            )
            .get_result::<models::PickedEvalJob>(&mut conn)
            .await
            .optional()?;

            match picked_job {
                Some(eval_job) => {
                    diesel::insert_into(concluded_eval_job_dsl::concluded_eval_job)
                        .values(&vec![models::ConcludedEvalJob { id: eval_job.id }])
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

        println!("got job {job_id} output:");
        for line in output {
            println!("  {line}");
        }

        Ok(Response::new(proto::server::JobOutputResponse {}))
    }
}

impl PubSubService {
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
                diesel::insert_into(eval_job_dsl::eval_job)
                    .values(&vec![Into::<models::EvalJob>::into(eval_job)])
                    .execute(&mut conn)
                    .await?;

                diesel::insert_into(pending_eval_job_dsl::pending_eval_job)
                    .values(&vec![models::PendingEvalJob { id: job_id }])
                    .execute(&mut conn)
                    .await?;

                Ok(())
            })
            .await
            .map_err(Into::<SubmitterError>::into)?;
        }

        for (builder_uuid, builder_state) in self.builder_states.write().await.iter_mut() {
            match builder_state.sender.push(eval_job.clone()).await {
                Some(_) => Err(SubmitterError::JobSend {
                    builder_uuid: *builder_uuid,
                }),
                None => Ok(()),
            }?
        }

        Ok(())
    }
}
