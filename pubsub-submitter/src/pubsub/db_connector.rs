use diesel::{
    AsChangeset, Connection as _, ExpressionMethods as _, Insertable, OptionalExtension as _,
    PgConnection, SelectableHelper as _,
    insertable::CanInsertInSingleQuery,
    pg::Pg,
    query_builder::{QueryFragment, QueryId},
    query_dsl::methods::{FilterDsl as _, SelectDsl as _},
    sql_query,
    sql_types::Text,
};
use diesel_async::{
    AsyncConnection as _, AsyncPgConnection, RunQueryDsl as _,
    pooled_connection::{
        AsyncDieselConnectionManager,
        deadpool::{Object, Pool, PoolError},
    },
};
use diesel_migrations::MigrationHarness as _;
use pubsub_proto::pubsub;
use serde::Deserialize;
use tokio_stream::StreamExt as _;
use tonic::Response;
use uuid::Uuid;

use crate::{
    error::{SubmitterError, ThroughProtocolError as _},
    models,
    pubsub::EvalJob,
    schema::{
        builder, concluded_eval_job, eval_job, job_output, pending_eval_job, picked_eval_job,
    },
};

pub const MIGRATIONS: diesel_migrations::EmbeddedMigrations =
    diesel_migrations::embed_migrations!("migrations");

#[derive(Clone)]
pub struct DBConnector {
    pg_pool: Pool<AsyncPgConnection>,
    database_url: String,
}

pub async fn insert_builder<S>(
    conn: &mut Object<AsyncPgConnection>,
    uuid: Uuid,
    system: S,
    smt_core_count: i32,
    total_memory: u64,
    active: bool,
) -> Result<(), SubmitterError>
where
    S: AsRef<str> + Send + Sync,
{
    conn.transaction::<_, SubmitterError, _>(async |mut conn| {
        diesel::insert_into(builder::dsl::builder)
            .values(&vec![models::Builder {
                id: uuid,
                system: system.as_ref().to_owned(),
                smt_core_count,
                // TODO down sizing this value
                total_memory: total_memory as i64,
                active,
            }])
            .execute(&mut conn)
            .await
            .expect("Failed to insert row");

        Ok(())
    })
    .await
    .map_err(Into::into)
}

pub async fn set_builder_active(
    conn: &mut Object<AsyncPgConnection>,
    uuid: Uuid,
    active: bool,
) -> Result<(), SubmitterError> {
    assert_eq!(
        1,
        diesel::update(builder::dsl::builder.filter(builder::dsl::id.eq(uuid)))
            .set(builder::dsl::active.eq(active))
            .execute(conn)
            .await
            .map_err(Into::<SubmitterError>::into)?
    );

    Ok(())
}

pub async fn insert_eval_jobs<U>(
    conn: &mut Object<AsyncPgConnection>,
    eval_jobs: U,
) -> Result<usize, SubmitterError>
where
    U: Insertable<eval_job::table>,
    <U as Insertable<eval_job::table>>::Values:
        QueryId + Send + CanInsertInSingleQuery<Pg> + QueryFragment<Pg>,
{
    diesel::insert_into(eval_job::dsl::eval_job)
        .values(eval_jobs)
        .execute(conn)
        .await
        .map_err(Into::into)
}

pub async fn insert_pending_jobs<U>(
    conn: &mut Object<AsyncPgConnection>,
    pending_jobs: U,
) -> Result<usize, SubmitterError>
where
    U: Insertable<pending_eval_job::table>,
    <U as Insertable<pending_eval_job::table>>::Values:
        QueryId + Send + CanInsertInSingleQuery<Pg> + QueryFragment<Pg>,
{
    diesel::insert_into(pending_eval_job::dsl::pending_eval_job)
        .values(pending_jobs)
        .execute(conn)
        .await
        .map_err(Into::into)
}

pub async fn get_pending_jobs(
    conn: &mut Object<AsyncPgConnection>,
) -> Result<Vec<EvalJob>, SubmitterError> {
    eval_job::dsl::eval_job
        .select(models::EvalJob::as_select())
        .load(conn)
        .await?
        .into_iter()
        .map(TryInto::<EvalJob>::try_into)
        .collect::<Result<Vec<EvalJob>, _>>()
        .map_err(Into::into)
}

pub async fn take_pending_job(
    conn: &mut Object<AsyncPgConnection>,
    uuid: Uuid,
) -> Result<Option<models::PendingEvalJob>, SubmitterError> {
    diesel::delete(
        pending_eval_job::dsl::pending_eval_job.filter(pending_eval_job::dsl::id.eq(uuid)),
    )
    .get_result::<models::PendingEvalJob>(conn)
    .await
    .optional()
    .map_err(Into::into)
}

pub async fn insert_picked_jobs<U>(
    conn: &mut Object<AsyncPgConnection>,
    picked_jobs: U,
) -> Result<usize, SubmitterError>
where
    U: Insertable<picked_eval_job::table>,
    <U as Insertable<picked_eval_job::table>>::Values:
        QueryId + Send + CanInsertInSingleQuery<Pg> + QueryFragment<Pg>,
{
    diesel::insert_into(picked_eval_job::dsl::picked_eval_job)
        .values(picked_jobs)
        .execute(conn)
        .await
        .map_err(Into::into)
}

pub async fn get_picked_job(
    conn: &mut Object<AsyncPgConnection>,
    uuid: Uuid,
) -> Result<Option<models::PickedEvalJob>, SubmitterError> {
    picked_eval_job::dsl::picked_eval_job
        .filter(picked_eval_job::dsl::id.eq(uuid))
        .select(models::PickedEvalJob::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(Into::into)
}

pub async fn take_picked_job(
    conn: &mut Object<AsyncPgConnection>,
    uuid: Uuid,
) -> Result<Option<models::PickedEvalJob>, SubmitterError> {
    diesel::delete(picked_eval_job::dsl::picked_eval_job.filter(picked_eval_job::dsl::id.eq(uuid)))
        .get_result::<models::PickedEvalJob>(conn)
        .await
        .optional()
        .map_err(Into::into)
}

pub async fn insert_concluded_jobs<U>(
    conn: &mut Object<AsyncPgConnection>,
    concluded_jobs: U,
) -> Result<usize, SubmitterError>
where
    U: Insertable<concluded_eval_job::table>,
    <U as Insertable<concluded_eval_job::table>>::Values:
        QueryId + Send + CanInsertInSingleQuery<Pg> + QueryFragment<Pg>,
{
    diesel::insert_into(concluded_eval_job::dsl::concluded_eval_job)
        .values(concluded_jobs)
        .execute(conn)
        .await
        .map_err(Into::into)
}

pub async fn insert_job_outputs<U>(
    conn: &mut Object<AsyncPgConnection>,
    job_outputs: U,
) -> Result<Vec<i64>, SubmitterError>
where
    U: Insertable<job_output::table>,
    <U as Insertable<job_output::table>>::Values:
        QueryId + Send + CanInsertInSingleQuery<Pg> + QueryFragment<Pg>,
{
    diesel::insert_into(job_output::dsl::job_output)
        .values(job_outputs)
        .returning(job_output::id)
        .get_results(conn)
        .await
        .map_err(Into::into)
}

pub async fn get_job_output(
    conn: &mut Object<AsyncPgConnection>,
    uuid: i64,
) -> Result<Option<models::JobOutput>, SubmitterError> {
    job_output::dsl::job_output
        .filter(job_output::dsl::id.eq(uuid))
        .select(models::JobOutput::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(Into::into)
}

pub async fn notify<S1, S2>(
    conn: &mut Object<AsyncPgConnection>,
    channel: S1,
    payload: S2,
) -> Result<usize, SubmitterError>
where
    S1: AsRef<str>,
    S2: AsRef<str>,
{
    // https://markandruth.co.uk/2022/12/20/using-postgres-notify-with-placeholders
    // thanks random person on the internet
    sql_query("SELECT pg_notify($1, $2)")
        .bind::<Text, _>(channel.as_ref())
        .bind::<Text, _>(payload.as_ref())
        .execute(conn)
        .await
        .map_err(Into::into)
}

pub enum DoContinue<T> {
    Continue,
    Stop(T),
}

pub async fn listen<S, F, T>(
    conn: &mut Object<AsyncPgConnection>,
    channel: S,
    mut callback: F,
) -> Result<(), SubmitterError>
where
    S: AsRef<str>,
    F: AsyncFnMut(T) -> DoContinue<Result<(), SubmitterError>>,
    for<'a> T: Deserialize<'a>,
{
    // safe as long as "channel" is not externally controlled
    // also proper substitutions dont seem to work with LISTEN and NOTIFY?
    sql_query(format!("LISTEN {}", channel.as_ref()))
        .execute(conn)
        .await
        .map_err(Into::<SubmitterError>::into)?;

    let mut notifs = std::pin::pin!(conn.notifications_stream());

    println!("listing to notifications on channel {}", channel.as_ref());

    loop {
        if let Some(notif) = notifs.next().await {
            let notif = notif.map_err(Into::<SubmitterError>::into)?;
            println!("got notification {:?}", notif);
            if notif.channel == channel.as_ref() {
                match serde_json::from_str::<T>(notif.payload.as_str()) {
                    Ok(t) => match callback(t).await {
                        DoContinue::Continue => {}
                        DoContinue::Stop(res) => break res,
                    },
                    Err(err) => Err(SubmitterError::SerdeError(err))?,
                };
            }
        } else {
            break Ok(());
        }
    }
}

impl DBConnector {
    pub fn new<S>(database_url: S) -> Result<Self, Box<dyn std::error::Error>>
    where
        S: AsRef<str>,
    {
        let config = AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(
            database_url.as_ref(),
        );
        let pool = Pool::builder(config).build()?;

        Ok(DBConnector {
            pg_pool: pool,
            database_url: database_url.as_ref().to_owned(),
        })
    }

    pub async fn get(&self) -> Result<Object<AsyncPgConnection>, SubmitterError> {
        self.pg_pool.get().await.map_err(Into::into)
    }

    pub fn run_migrations(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = PgConnection::establish(&self.database_url)?;
        conn.run_pending_migrations(MIGRATIONS)?;

        Ok(())
    }
}
