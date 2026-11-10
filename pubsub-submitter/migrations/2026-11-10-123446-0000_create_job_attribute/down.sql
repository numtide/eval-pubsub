-- This file should undo anything in `up.sql`

DROP TABLE job_attribute;

ALTER TABLE job_output
    DROP COLUMN attr_id;
