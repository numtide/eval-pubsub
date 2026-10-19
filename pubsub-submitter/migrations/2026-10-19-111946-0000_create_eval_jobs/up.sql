CREATE TYPE eval_job_type AS ENUM (
    'flake'
);

CREATE TABLE eval_job (
    id uuid,
    type eval_job_type NOT NULL,
    flake_ref text NULL,
    attrs text[] NULL,
    PRIMARY KEY ( id ),
    CONSTRAINT adt CHECK (
        CASE type
            WHEN 'flake' THEN flake_ref IS NOT NULL AND attrs IS NOT NULL
        END
    ),
    CONSTRAINT check_attrs_non_null CHECK (
        CASE
            WHEN attrs IS NOT NULL THEN array_position(attrs, null) is null
        END
    )
);


CREATE TABLE pending_eval_job (
    id uuid references eval_job(id),
    PRIMARY KEY ( id )
);

CREATE TABLE picked_eval_job (
    id uuid unique references eval_job(id),
    PRIMARY KEY ( id )
);

CREATE TABLE concluded_eval_job (
    id uuid references eval_job(id),
    PRIMARY KEY ( id )
);
