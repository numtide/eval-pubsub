-- This file should undo anything in `up.sql`
UPDATE job_output SET time = NULL WHERE time IS TIMESTAMP '1970-01-01 00:00:00';
ALTER TABLE job_output ALTER COLUMN time DROP NOT NULL;
