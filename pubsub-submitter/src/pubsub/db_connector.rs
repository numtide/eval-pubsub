use diesel::{
    Connection as _, ExpressionMethods as _, Insertable, OptionalExtension as _, PgConnection,
    SelectableHelper as _,
    insertable::CanInsertInSingleQuery,
    pg::Pg,
    query_builder::{QueryFragment, QueryId},
    query_dsl::QueryDsl as _,
    sql_query,
    sql_types::Text,
};
use diesel_async::{
    AsyncConnection as _, AsyncPgConnection, RunQueryDsl as _,
    pooled_connection::{
        AsyncDieselConnectionManager,
        deadpool::{Object, Pool},
    },
};
use diesel_migrations::MigrationHarness as _;
use tokio_stream::StreamExt as _;
use uuid::Uuid;

use crate::{
    db,
    error::SubmitterError,
    models,
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

pub async fn insert_builders<I>(
    conn: &mut Object<AsyncPgConnection>,
    builders: I,
) -> Result<(), SubmitterError>
where
    I: IntoIterator<Item = models::Builder>,
{
    diesel::insert_into(builder::dsl::builder)
        .values(
            builders
                .into_iter()
                .map(Into::<db::models::Builder>::into)
                .collect::<Vec<_>>(),
        )
        .execute(conn)
        .await
        .expect("Failed to insert row");

    Ok(())
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

pub async fn get_job(
    conn: &mut Object<AsyncPgConnection>,
    uuid: Uuid,
) -> Result<Option<models::Job>, SubmitterError> {
    eval_job::dsl::eval_job
        .filter(eval_job::dsl::id.eq(uuid))
        .select(db::models::EvalJob::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(Into::into)
        .and_then(|eval_job| {
            eval_job
                .map(TryInto::try_into)
                .transpose()
                .map_err(Into::into)
        })
}

pub async fn get_jobs(
    conn: &mut Object<AsyncPgConnection>,
) -> Result<Vec<models::Job>, SubmitterError> {
    eval_job::dsl::eval_job
        .select(db::models::EvalJob::as_select())
        .load(conn)
        .await?
        .into_iter()
        .map(TryInto::try_into)
        .collect::<Result<Vec<_>, _>>()
        .map_err(Into::into)
}

pub async fn get_eval_jobs_paginated(
    conn: &mut Object<AsyncPgConnection>,
    page: i64,
    count: i64,
) -> Result<(Vec<models::Job>, i64), SubmitterError> {
    conn.transaction(async |mut conn| {
        let eval_jobs = eval_job::dsl::eval_job
            .select(db::models::EvalJob::as_select())
            .limit(count)
            .offset(page * count)
            .load(&mut conn)
            .await?
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?
            .into();
        let total = eval_job::dsl::eval_job
            .select(diesel::dsl::count(eval_job::dsl::id))
            .first(&mut conn)
            .await?;

        Ok((eval_jobs, total))
    })
    .await
}

pub async fn insert_jobs<I>(
    conn: &mut Object<AsyncPgConnection>,
    jobs: I,
) -> Result<usize, SubmitterError>
where
    I: IntoIterator<Item = models::Job>,
{
    diesel::insert_into(eval_job::dsl::eval_job)
        .values(
            jobs.into_iter()
                .map(Into::<db::models::EvalJob>::into)
                .collect::<Vec<_>>(),
        )
        .execute(conn)
        .await
        .map_err(Into::into)
}

pub async fn insert_pending_jobs<I>(
    conn: &mut Object<AsyncPgConnection>,
    pending_jobs: I,
) -> Result<usize, SubmitterError>
where
    I: IntoIterator<Item = Uuid>,
{
    diesel::insert_into(pending_eval_job::dsl::pending_eval_job)
        .values(
            pending_jobs
                .into_iter()
                .map(|uuid| db::models::PendingEvalJob { id: uuid })
                .collect::<Vec<_>>(),
        )
        .execute(conn)
        .await
        .map_err(Into::into)
}

pub async fn get_pending_jobs(
    conn: &mut Object<AsyncPgConnection>,
) -> Result<Vec<models::Job>, SubmitterError> {
    eval_job::table
        .inner_join(pending_eval_job::table)
        .select(db::models::EvalJob::as_select())
        .load(conn)
        .await?
        .into_iter()
        .map(TryInto::try_into)
        .collect::<Result<Vec<_>, _>>()
        .map_err(Into::into)
}

pub async fn take_pending_job(
    conn: &mut Object<AsyncPgConnection>,
    uuid: Uuid,
) -> Result<Option<models::Job>, SubmitterError> {
    conn.transaction(async |conn| {
        let pending_job = diesel::delete(
            pending_eval_job::dsl::pending_eval_job.filter(pending_eval_job::dsl::id.eq(uuid)),
        )
        .get_result::<db::models::PendingEvalJob>(conn)
        .await
        .optional()?;

        match pending_job {
            Some(pending_job) => get_job(conn, pending_job.id).await,
            None => Ok(None),
        }
    })
    .await
}

pub async fn insert_picked_jobs<I>(
    conn: &mut Object<AsyncPgConnection>,
    picked_jobs: I,
) -> Result<usize, SubmitterError>
where
    I: IntoIterator<Item = models::PickedJob>,
{
    diesel::insert_into(picked_eval_job::dsl::picked_eval_job)
        .values(
            picked_jobs
                .into_iter()
                .map(|picked_job| Into::<db::models::PickedEvalJob>::into(picked_job))
                .collect::<Vec<_>>(),
        )
        .execute(conn)
        .await
        .map_err(Into::into)
}

pub async fn get_picked_job(
    conn: &mut Object<AsyncPgConnection>,
    uuid: Uuid,
) -> Result<Option<models::PickedJob>, SubmitterError> {
    picked_eval_job::dsl::picked_eval_job
        .filter(picked_eval_job::dsl::id.eq(uuid))
        .select(db::models::PickedEvalJob::as_select())
        .first(conn)
        .await
        .optional()
        .map(|picked_job| picked_job.map(Into::into))
        .map_err(Into::into)
}

pub async fn take_picked_job(
    conn: &mut Object<AsyncPgConnection>,
    uuid: Uuid,
) -> Result<Option<models::PickedJob>, SubmitterError> {
    diesel::delete(picked_eval_job::dsl::picked_eval_job.filter(picked_eval_job::dsl::id.eq(uuid)))
        .get_result::<db::models::PickedEvalJob>(conn)
        .await
        .optional()
        .map(|picked_job| picked_job.map(Into::into))
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
) -> Result<Option<db::models::JobOutput>, SubmitterError> {
    job_output::dsl::job_output
        .filter(job_output::dsl::id.eq(uuid))
        .select(db::models::JobOutput::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(Into::into)
}

pub async fn get_builder(
    conn: &mut Object<AsyncPgConnection>,
    uuid: Uuid,
) -> Result<Option<db::models::Builder>, SubmitterError> {
    builder::dsl::builder
        .filter(builder::dsl::id.eq(uuid))
        .select(db::models::Builder::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(Into::into)
}

pub trait PgNotifiable {
    fn channel() -> String;
    fn to_json(&self) -> Result<String, serde_json::error::Error>;
    fn from_json<S>(string: S) -> Result<Self, serde_json::error::Error>
    where
        S: AsRef<str>,
        Self: Sized;
}

pub async fn notify<T>(
    conn: &mut Object<AsyncPgConnection>,
    payload: T,
) -> Result<usize, SubmitterError>
where
    T: PgNotifiable,
{
    let payload = payload.to_json()?;
    let channel = T::channel();
    log::debug!("pg_notify on channel {} with payload {}", channel, payload);
    // https://markandruth.co.uk/2022/12/20/using-postgres-notify-with-placeholders
    // thanks random person on the internet
    sql_query("SELECT pg_notify($1, $2)")
        .bind::<Text, _>(channel.as_str())
        .bind::<Text, _>(payload)
        .execute(conn)
        .await
        .map_err(Into::into)
}

pub enum DoContinue<T> {
    Continue,
    Stop(T),
}

pub async fn listen<F, T>(
    conn: &mut Object<AsyncPgConnection>,
    mut callback: F,
) -> Result<(), SubmitterError>
where
    F: AsyncFnMut(T) -> DoContinue<Result<(), SubmitterError>>,
    T: PgNotifiable,
{
    let channel = T::channel();
    // safe because channel is essentially a constant
    sql_query(format!("LISTEN {}", channel))
        .execute(conn)
        .await
        .map_err(Into::<SubmitterError>::into)?;

    let mut notifs = std::pin::pin!(conn.notifications_stream());

    log::debug!("pg_listen on channel {}", channel);

    loop {
        if let Some(notif) = notifs.next().await {
            let notif = notif.map_err(Into::<SubmitterError>::into)?;
            log::debug!("got notification {:?} on channel {}", notif, channel);
            if notif.channel == T::channel() {
                match T::from_json(notif.payload) {
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
