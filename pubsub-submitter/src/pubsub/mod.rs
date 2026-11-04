#![allow(unstable_name_collisions)]
use diesel::backend::Backend;
use diesel::deserialize::{FromSql, Queryable, QueryableByName};
use diesel::{IntoSql as _, sql_types};
use diesel_async::pooled_connection::deadpool::Object;
use diesel_async::{AsyncConnection as _, AsyncPgConnection, RunQueryDsl as _};
use pubsub_proto::pubsub as proto;
use pubsub_proto::pubsub::client::pub_sub_client_server::PubSubClient;
use pubsub_proto::pubsub::server::pub_sub_server::PubSub;

use itertools::Itertools;
use std::panic;
use std::sync::Arc;
use tokio::sync::{RwLock, SetOnce, mpsc};
use tokio::task::{JoinHandle, JoinSet};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::db;
use crate::error::ThroughProtocolError;
use crate::error::{ProtocolError, SubmitterError};
use crate::models;
use crate::pubsub::concluded_job_notification::ConcludedJobNotification;
use crate::pubsub::db_connector::DoContinue;
use crate::pubsub::job_output_notification::JobOutputNotification;
use crate::pubsub::pending_job_notification::PendingJobNotification;
use db_connector::DBConnector;

mod concluded_job_notification;
mod db_connector;
mod job_output_notification;
mod pending_job_notification;

#[derive(Clone)]
pub struct PubSubService {
    db_connector: DBConnector,
    join_set: Arc<RwLock<JoinSet<()>>>,
}

impl PubSubService {
    async fn check_registered(
        &self,
        conn: &mut Object<AsyncPgConnection>,
        builder_uuid: Uuid,
    ) -> Result<(), SubmitterError> {
        match db_connector::get_builder(conn, builder_uuid).await? {
            Some(_builder) => Ok(()),
            None => Err(SubmitterError::Unregistered {}),
        }
    }

    pub async fn spawn<S, F1, F2>(&self, name: S, future: F1, finally: F2) -> JoinHandle<F2::Output>
    where
        S: AsRef<str>,
        F1: Future<Output = ()> + Send + 'static,
        F2: Future<Output = ()> + Send + 'static,
    {
        let name = name.as_ref().to_owned();
        log::trace!("spawning background task with name {}", name);
        let join_set = self.join_set.clone();
        tokio::spawn(async move {
            join_set.write().await.spawn(async move {
                future.await;
                log::trace!("background task with name {} exiting", name);
                finally.await
            });
        })
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

        log::debug!(
            "builder {} registered: {:?}",
            builder_uuid,
            builder_information
        );

        db_connector::insert_builders(
            &mut conn,
            models::Builder::new_from_uuid_and_system(&builder_uuid, &builder_information.system),
        )
        .await?;

        Ok(Response::new(proto::server::RegisterResponse {}))
    }

    async fn list_jobs(
        &self,
        request: Request<proto::server::ListJobsRequest>,
    ) -> Result<Response<Self::ListJobsStream>, Status> {
        let mut conn = self.db_connector.get().await.through()?;
        let mut conn_listener = self.db_connector.get().await.through()?;
        let mut conn_notifs = self.db_connector.get().await.through()?;
        let builder_uuid = get_uuid_from_request(&request)?;
        self.check_registered(&mut conn, builder_uuid).await?;

        let (tx, rx) = mpsc::channel(1);

        db_connector::set_builder_active(&mut conn, builder_uuid, true).await?;

        {
            let tx = tx.clone();
            self.spawn(
                "list_jobs",
                async move {
                    db_connector::listen(
                        &mut conn_notifs,
                        async move |pending_job: PendingJobNotification| {
                            match db_connector::get_job(&mut conn_listener, pending_job.job_uuid)
                                .await
                            {
                                Ok(Some(job)) => {
                                    log::trace!(
                                        "sending new {:?} to builder {}",
                                        job,
                                        builder_uuid
                                    );
                                    tx.send(Ok(proto::server::ListJobsResponse {
                                        job: Some(job.into()),
                                    }))
                                    .await
                                    .map_err(|_| SubmitterError::JobSend { builder_uuid })
                                    .unwrap();

                                    DoContinue::Continue
                                }
                                Ok(None) => todo!(),
                                Err(err) => DoContinue::Stop(Err(err)),
                            }
                        },
                    )
                    .await
                    .unwrap();
                },
                async move {},
            )
            .await
        };

        let pending_jobs = db_connector::get_pending_jobs(&mut conn).await?;

        for pending_job in pending_jobs {
            log::trace!(
                "sending pending {:?} to builder {}",
                pending_job,
                builder_uuid
            );
            tx.send(Ok(proto::server::ListJobsResponse {
                job: Some(pending_job.clone().into()),
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

        log::info!("builder {} picking job with UUID {}", builder_uuid, job_id);

        conn.transaction::<_, SubmitterError, _>(async |mut conn| {
            let pending_job = db_connector::take_pending_job(&mut conn, job_id).await?;

            match pending_job {
                Some(pending_job) => {
                    db_connector::insert_picked_jobs(
                        conn,
                        [models::PickedJob::new(pending_job.job_id, builder_uuid)],
                    )
                    .await?;

                    Ok(Response::new(proto::server::PickJobResponse {
                        pick_response: Some(proto::server::pick_job_response::PickResponse::Allow(
                            proto::server::pick_job_response::Allow {},
                        )),
                    }))
                }
                None => match db_connector::get_picked_job(&mut conn, job_id).await? {
                    Some(_) => Ok(Response::new(proto::server::PickJobResponse {
                        pick_response: Some(proto::server::pick_job_response::PickResponse::Deny(
                            proto::server::pick_job_response::Deny {},
                        )),
                    })),
                    None => Err(SubmitterError::JobNotFound { job_uuid: job_id }.into()),
                },
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

        log::info!(
            "builder {} concluding job with UUID {}",
            builder_uuid,
            job_id
        );

        conn.transaction::<_, SubmitterError, _>(async |mut conn| {
            match db_connector::take_picked_job(&mut conn, job_id).await? {
                Some(eval_job) => {
                    db_connector::insert_concluded_jobs(
                        &mut conn,
                        &vec![db::models::ConcludedEvalJob {
                            id: eval_job.id,
                            builder_id: builder_uuid,
                        }],
                    )
                    .await?;
                    db_connector::notify(&mut conn, ConcludedJobNotification::new(job_id)).await?;

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
                    .map(|line| db::models::NewJobOutput {
                        job_id: job_uuid,
                        line,
                    })
                    .collect::<Vec<_>>(),
            )
            .await?;
            for job_output_id in uuids {
                let job_output_notification = JobOutputNotification::new(job_uuid, job_output_id);
                db_connector::notify(&mut conn, job_output_notification).await?;
            }

            log::info!(
                "got output for job with UUID {}:\n{}",
                job_uuid,
                output
                    .iter()
                    .map(|line| format!("- {}", line))
                    .intersperse("\n".to_owned())
                    .collect::<String>()
            );
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
        let anonymous_job = request
            .get_ref()
            .job
            .clone()
            .ok_or(ProtocolError::field_missing(&["job"]))
            .and_then(TryInto::<models::AnonymousJob>::try_into)?;
        let job = models::Job::new(anonymous_job);
        let job_id = job.job_id;

        log::info!("client submitted job {:?}", job);

        self.submit_job(job).await?;

        Ok(Response::new(proto::client::SubmitJobResponse {
            job_id: Some(job_id.into()),
        }))
    }

    async fn tail_job(
        &self,
        request: Request<proto::client::TailJobRequest>,
    ) -> Result<Response<Self::TailJobStream>, Status> {
        let mut conn = self.db_connector.get().await?;
        let mut conn_notifs = self.db_connector.get().await?;
        let job_uuid: Uuid = request
            .get_ref()
            .job_id
            .as_ref()
            .ok_or(ProtocolError::field_missing(&["job_id"]))?
            .into();

        let (tx, rx) = mpsc::channel(1);

        log::info!("client tailing job with UUID {}", job_uuid);

        self.spawn(
            "tail_job",
            async move {
                db_connector::listen(
                    &mut conn_notifs,
                    async move |job_output_notification: JobOutputNotification| {
                        if job_output_notification.job_uuid == job_uuid {
                            match db_connector::get_job_output(
                                &mut conn,
                                job_output_notification.job_output_id,
                            )
                            .await
                            .unwrap()
                            {
                                Some(job_output) => {
                                    log::trace!(
                                        "sending job output {:?} for joib with UUID {}",
                                        job_output,
                                        job_uuid
                                    );
                                    tx.send(Ok(proto::client::TailJobResponse {
                                        line: job_output.line,
                                    }))
                                    .await
                                    .unwrap();
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

                        DoContinue::Continue
                    },
                )
                .await
                .unwrap()
            },
            async move {},
        )
        .await;

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn wait_job(
        &self,
        request: Request<proto::client::WaitJobRequest>,
    ) -> Result<Response<proto::client::WaitJobResponse>, Status> {
        let job_uuid: Uuid = request
            .get_ref()
            .job_id
            .as_ref()
            .ok_or(ProtocolError::field_missing(&["job_id"]))?
            .into();
        let result = Arc::new(SetOnce::new());
        let mut conn_notifs = self.db_connector.get().await?;

        log::info!("client waiting for job with UUID {}", job_uuid);

        {
            let result = result.clone();
            db_connector::listen(
                &mut conn_notifs,
                async move |concluded_job_notification: ConcludedJobNotification| {
                    if concluded_job_notification.job_uuid == job_uuid {
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
        let mut conn = self.db_connector.get().await?;
        let page = request.get_ref().page;
        let count = request.get_ref().count;

        log::info!("client listing all jobs");

        db_connector::get_eval_jobs_paginated(&mut conn, page as i64, count as i64)
            .await
            .map(|(jobs, total)| {
                Response::new(proto::client::ListJobsResponse {
                    jobs: jobs.into_iter().map(Into::into).collect(),
                    total: total as i32,
                })
            })
            .map_err(Into::into)
    }

    async fn list_builders(
        &self,
        request: Request<proto::client::ListBuildersRequest>,
    ) -> Result<Response<proto::client::ListBuildersResponse>, Status> {
        log::info!("client listing builders");

        Err(Status::unimplemented("todo"))
    }
}

struct DatabaseName {
    name: String,
}

impl<DB> Queryable<sql_types::Text, DB> for DatabaseName
where
    DB: Backend,
    String: FromSql<sql_types::Text, DB>,
{
    type Row = String;

    fn build(name: Self::Row) -> diesel::deserialize::Result<Self> {
        Ok(Self { name })
    }
}

impl PubSubService {
    pub async fn wait_shutdown(self) {
        let mut join_set = self.join_set.write().await;
        join_set.abort_all();
        while let Some(res) = join_set.join_next().await {
            match res {
                Ok(_) => {}
                Err(err) if err.is_cancelled() => {}
                Err(err) => panic::resume_unwind(err.into_panic()),
            };
        }
    }

    pub async fn new<S>(database_url: S) -> Result<PubSubService, Box<dyn std::error::Error>>
    where
        S: AsRef<str>,
    {
        let (db_connector, connection) = DBConnector::new(database_url.as_ref().to_owned()).await?;
        let pub_sub_service = PubSubService {
            join_set: Arc::new(RwLock::new(JoinSet::new())),
            db_connector,
        };
        pub_sub_service
            .spawn(
                "pg_client connection",
                async move {
                    log::debug!("starting one raw PSQL connection");
                    connection.await.unwrap();
                    log::debug!("exiting? PSQL connection");
                },
                async {},
            )
            .await;

        Ok(pub_sub_service)
    }

    pub fn run_migrations(
        &mut self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        self.db_connector.run_migrations()
    }

    pub async fn clear_db(&mut self) {
        let mut conn = self.db_connector.get().await.through().unwrap();
        if log::log_enabled!(log::Level::Info) {
            let dbname: String = self
                .db_connector
                .pg_client
                .write()
                .await
                .query_one("select current_database();", &[])
                .await
                .unwrap()
                .get(0);
            log::info!("clearing database {}", dbname);
        };

        diesel::sql_query("DROP SCHEMA public CASCADE;")
            .execute(&mut conn)
            .await
            .unwrap();
        diesel::sql_query("CREATE SCHEMA public;")
            .execute(&mut conn)
            .await
            .unwrap();
    }

    pub async fn submit_job(&self, job: models::Job) -> Result<(), SubmitterError> {
        let mut conn = self.db_connector.get().await.through()?;
        {
            let job = job.clone();

            conn.transaction::<_, SubmitterError, _>(async |mut conn| {
                let job_uuid = job.job_id;
                log::debug!("submitting job {:?}", job);

                db_connector::insert_jobs(&mut conn, [job]).await?;
                db_connector::insert_pending_jobs(&mut conn, [job_uuid]).await?;
                db_connector::notify(&mut conn, PendingJobNotification::new(job_uuid)).await?;

                Ok(())
            })
            .await
            .map_err(Into::<SubmitterError>::into)?;
        }

        Ok(())
    }
}
