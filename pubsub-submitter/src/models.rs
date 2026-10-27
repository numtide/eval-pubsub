use std::io::Write as _;

use diesel::{
    Selectable,
    deserialize::{self, FromSql, FromSqlRow, Queryable},
    expression::AsExpression,
    prelude::Insertable,
    serialize::{IsNull, ToSql},
};
use uuid::Uuid;

use crate::schema::{eval_job, sql_types};

type DB = diesel::pg::Pg;

#[derive(Debug, PartialEq, FromSqlRow, Eq, AsExpression)]
#[diesel(sql_type = sql_types::EvalJobType)]
pub enum EvalJobType {
    Flake,
}

impl FromSql<sql_types::EvalJobType, DB> for EvalJobType {
    fn from_sql(
        bytes: <DB as diesel::backend::Backend>::RawValue<'_>,
    ) -> deserialize::Result<Self> {
        match bytes.as_bytes() {
            b"flake" => Ok(EvalJobType::Flake),
            _ => Err("Unrecognized role variant".into()),
        }
    }
}

impl ToSql<sql_types::EvalJobType, DB> for EvalJobType {
    fn to_sql<'b>(
        &'b self,
        out: &mut diesel::serialize::Output<'b, '_, DB>,
    ) -> diesel::serialize::Result {
        match *self {
            EvalJobType::Flake => out.write_all(b"flake")?,
        }
        Ok(IsNull::No)
    }
}

#[derive(Queryable, Selectable, Debug, Insertable)]
#[diesel(table_name = crate::schema::pending_eval_job)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct PendingEvalJob {
    pub id: Uuid,
}

#[derive(Queryable, Selectable, Debug, Insertable)]
#[diesel(table_name = crate::schema::picked_eval_job)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct PickedEvalJob {
    pub id: Uuid,
    pub builder_id: Uuid,
}

#[derive(Queryable, Selectable, Debug, Insertable)]
#[diesel(table_name = crate::schema::concluded_eval_job)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct ConcludedEvalJob {
    pub id: Uuid,
    pub builder_id: Uuid,
}

#[derive(Queryable, Selectable, Debug, Insertable)]
#[diesel(table_name = crate::schema::job_output)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct JobOutput {
    pub id: i64,
    pub job_id: Uuid,
    pub line: String,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::job_output)]
pub struct NewJobOutput<'a> {
    pub job_id: Uuid,
    pub line: &'a str,
}

#[derive(Queryable, Selectable, Debug, Insertable)]
#[diesel(table_name = crate::schema::eval_job)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct EvalJob {
    pub id: Uuid,
    pub type_: EvalJobType,
    pub flake_ref: Option<String>,
    pub attrs: Option<Vec<Option<String>>>,
}

impl From<super::pubsub::EvalJob> for EvalJob {
    fn from(value: super::pubsub::EvalJob) -> Self {
        match value {
            crate::pubsub::EvalJob {
                job_id,
                r#type: crate::pubsub::EvalJobType::Flake { flake_ref, attrs },
            } => EvalJob {
                id: job_id,
                type_: EvalJobType::Flake,
                flake_ref: Some(flake_ref),
                attrs: Some(attrs.into_iter().map(Some).collect()),
            },
        }
    }
}

impl TryFrom<EvalJob> for super::pubsub::EvalJob {
    type Error = ();

    fn try_from(value: EvalJob) -> Result<Self, ()> {
        match value {
            EvalJob {
                id,
                type_: EvalJobType::Flake,
                flake_ref,
                attrs,
            } => Ok(crate::pubsub::EvalJob {
                job_id: id,
                r#type: crate::pubsub::EvalJobType::Flake {
                    flake_ref: flake_ref.ok_or(())?,
                    attrs: attrs
                        .ok_or(())?
                        .into_iter()
                        .collect::<Option<_>>()
                        .ok_or(())?,
                },
            }),
        }
    }
}

#[derive(Queryable, Selectable, Debug, Insertable)]
#[diesel(table_name = crate::schema::builder)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Builder {
    pub id: Uuid,
    pub system: String,
    pub smt_core_count: i32,
    // TODO this really ought to be u64 but that would need a postgresl extension
    pub total_memory: i64,
    pub active: bool,
}
