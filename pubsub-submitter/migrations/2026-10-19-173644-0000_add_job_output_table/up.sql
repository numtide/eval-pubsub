-- Your SQL goes here

CREATE TABLE job_output (
    id bigserial NOT NULL,
    job_id uuid NOT NULL REFERENCES eval_job(id),
    time timestamp without time zone,
    line text NOT NULL,
    PRIMARY KEY ( id )
);
