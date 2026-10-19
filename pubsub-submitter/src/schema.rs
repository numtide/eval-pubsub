// @generated automatically by Diesel CLI.

pub mod sql_types {
    #[derive(diesel::query_builder::QueryId, Clone, diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "eval_job_type"))]
    pub struct EvalJobType;
}

diesel::table! {
    concluded_eval_job (id) {
        id -> Uuid,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::EvalJobType;

    eval_job (id) {
        id -> Uuid,
        #[sql_name = "type"]
        type_ -> EvalJobType,
        flake_ref -> Nullable<Text>,
        attrs -> Nullable<Array<Nullable<Text>>>,
    }
}

diesel::table! {
    job_output (id) {
        id -> Int8,
        job_id -> Uuid,
        time -> Nullable<Timestamp>,
        line -> Text,
    }
}

diesel::table! {
    pending_eval_job (id) {
        id -> Uuid,
    }
}

diesel::table! {
    picked_eval_job (id) {
        id -> Uuid,
    }
}

diesel::joinable!(concluded_eval_job -> eval_job (id));
diesel::joinable!(job_output -> eval_job (job_id));
diesel::joinable!(pending_eval_job -> eval_job (id));
diesel::joinable!(picked_eval_job -> eval_job (id));

diesel::allow_tables_to_appear_in_same_query!(
    concluded_eval_job,
    eval_job,
    job_output,
    pending_eval_job,
    picked_eval_job,
);
