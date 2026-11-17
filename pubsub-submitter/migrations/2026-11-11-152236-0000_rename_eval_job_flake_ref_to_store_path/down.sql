-- This file should undo anything in `up.sql`

ALTER TABLE eval_job
  RENAME COLUMN store_path TO flake_ref;
