-- Your SQL goes here

ALTER TABLE eval_job
  RENAME COLUMN flake_ref TO store_path;
