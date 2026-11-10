-- Your SQL goes here

CREATE TABLE job_attribute (
    id bigserial NOT NULL,
    attr_path text[] NOT NULL UNIQUE,
    PRIMARY KEY ( id ),
    CONSTRAINT check_attr_path_non_null CHECK (
        array_position(attr_path, null) IS NULL
    )
);

ALTER TABLE job_output
    ADD COLUMN attr_id bigserial REFERENCES job_attribute(id);
ALTER TABLE job_output
    ALTER COLUMN attr_id DROP NOT NULL;
