-- This file should undo anything in `up.sql`
UPDATE job_output SET time = NULL WHERE time = '1970-01-01 00:00:00'::timestamp;
ALTER TABLE job_output ALTER COLUMN time DROP NOT NULL;
