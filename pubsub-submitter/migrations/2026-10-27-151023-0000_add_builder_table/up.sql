-- Your SQL goes here

CREATE TABLE builder (
    id uuid NOT NULL,
    system text NOT NULL,
    smt_core_count integer NOT NULL,
    total_memory bigint NOT NULL,
    active bool NOT NULL,
    PRIMARY KEY ( id )
);
