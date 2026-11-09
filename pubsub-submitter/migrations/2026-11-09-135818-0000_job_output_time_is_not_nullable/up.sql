-- Your SQL goes here
UPDATE job_output SET time = TIMESTAMP '1970-01-01 00:00:00' WHERE time IS NULL;
ALTER TABLE job_output ALTER COLUMN time SET NOT NULL;
