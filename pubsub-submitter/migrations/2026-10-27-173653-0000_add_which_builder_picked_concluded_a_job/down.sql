-- This file should undo anything in `up.sql`

ALTER TABLE picked_eval_job
  DROP COLUMN builder_id,
  DROP CONSTRAINT builder_id;

ALTER TABLE concluded_eval_job
  DROP COLUMN builder_id,
  DROP CONSTRAINT builder_id;
