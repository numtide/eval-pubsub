use std::io::Write as _;

use diesel::{
    Selectable,
    deserialize::{self, FromSql, FromSqlRow, Queryable, QueryableByName},
    expression::AsExpression,
    prelude::Insertable,
    serialize::{IsNull, ToSql},
};
use uuid::Uuid;

use crate::{
    models,
    schema::{eval_job, sql_types},
};

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

impl From<models::PickedJob> for PickedEvalJob {
    fn from(value: models::PickedJob) -> Self {
        let models::PickedJob { id, builder_id } = value;
        Self { id, builder_id }
    }
}

impl From<PickedEvalJob> for models::PickedJob {
    fn from(value: PickedEvalJob) -> Self {
        let PickedEvalJob { id, builder_id } = value;
        Self { id, builder_id }
    }
}

#[derive(Queryable, Selectable, Debug, Insertable)]
#[diesel(table_name = crate::schema::concluded_eval_job)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct ConcludedEvalJob {
    pub id: Uuid,
    pub builder_id: Uuid,
}

impl From<models::ConcludedJob> for ConcludedEvalJob {
    fn from(value: models::ConcludedJob) -> Self {
        let models::ConcludedJob { id, builder_id } = value;
        Self { id, builder_id }
    }
}

impl From<ConcludedEvalJob> for models::ConcludedJob {
    fn from(value: ConcludedEvalJob) -> Self {
        let ConcludedEvalJob { id, builder_id } = value;
        Self { id, builder_id }
    }
}

#[derive(Queryable, Selectable, Debug, Insertable, QueryableByName)]
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

impl From<models::Job> for EvalJob {
    fn from(value: models::Job) -> Self {
        match value {
            models::Job {
                job_id,
                anonymous_job:
                    models::AnonymousJob::EvalJob {
                        eval_job: models::EvalJob::Flake { flake_ref, attrs },
                    },
            } => EvalJob {
                id: job_id,
                type_: EvalJobType::Flake,
                flake_ref: Some(flake_ref),
                attrs: Some(attrs.into_iter().map(Some).collect()),
            },
            _ => panic!("database doesn't support non-flake non-eval jobs currently"),
        }
    }
}

impl TryFrom<EvalJob> for models::Job {
    type Error = ();

    fn try_from(value: EvalJob) -> Result<Self, ()> {
        match value {
            EvalJob {
                id,
                type_: EvalJobType::Flake,
                flake_ref,
                attrs,
            } => Ok(models::Job {
                job_id: id,
                anonymous_job: models::AnonymousJob::EvalJob {
                    eval_job: models::EvalJob::Flake {
                        flake_ref: flake_ref.ok_or(())?,
                        attrs: attrs
                            .ok_or(())?
                            .into_iter()
                            .collect::<Option<_>>()
                            .ok_or(())?,
                    },
                },
            }),
        }
    }
}

#[derive(QueryableByName)]
pub struct EvalJobWithStatus {
    #[diesel(column_name = "id", sql_type = diesel::sql_types::Uuid)]
    pub uuid: Uuid,
    #[diesel(column_name = "type", sql_type = diesel::sql_types::Text)]
    pub r#type: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub flake_ref: String,
    #[diesel(sql_type = diesel::sql_types::Array<diesel::sql_types::Text>)]
    pub attrs: Vec<String>,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    pub pending: bool,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    pub picked: bool,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    pub concluded: bool,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Uuid>)]
    pub concluded_builder_uuid: Option<Uuid>,
}

impl TryFrom<EvalJobWithStatus> for models::JobWithStatus {
    type Error = ();

    fn try_from(value: EvalJobWithStatus) -> Result<Self, Self::Error> {
        assert!(value.picked ^ value.pending ^ value.concluded);
        Ok(Self {
            job: models::Job {
                job_id: value.uuid,
                anonymous_job: models::AnonymousJob::EvalJob {
                    eval_job: match value.r#type.as_str() {
                        "flake" => models::EvalJob::Flake {
                            flake_ref: value.flake_ref,
                            attrs: value.attrs,
                        },
                        _ => Err(())?,
                    },
                },
            },
            status: if value.pending {
                models::JobStatus::Pending
            } else if value.picked {
                models::JobStatus::Picked
            } else if value.concluded {
                log::warn!("for now always reporting jobs as succeeded");
                models::JobStatus::Suceeded {
                    builder_uuid: value
                        .concluded_builder_uuid
                        .expect("concluded_builder_uuid must be set when a job is concluded"),
                }
            } else {
                log::error!(
                    "job {} was neither pending, picked, nor concluded",
                    value.uuid
                );
                Err(())?
            },
        })
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
    pub store_uri: String,
    pub active: bool,
}

impl From<Builder> for models::Builder {
    fn from(value: Builder) -> Self {
        let Builder {
            system,
            smt_core_count,
            active,
            store_uri,
            ..
        } = value;
        Self {
            uuid: value.id,
            system,
            smt_core_count,
            total_memory: value.total_memory as u64,
            store_uri,
            active,
        }
    }
}

impl From<models::Builder> for Builder {
    fn from(value: models::Builder) -> Self {
        let models::Builder {
            system,
            smt_core_count,
            active,
            store_uri,
            ..
        } = value;
        Self {
            id: value.uuid,
            system,
            smt_core_count,
            total_memory: value.total_memory as i64,
            active,
            store_uri,
        }
    }
}
