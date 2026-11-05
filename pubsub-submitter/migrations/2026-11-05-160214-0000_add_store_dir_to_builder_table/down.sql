-- This file should undo anything in `up.sql`

ALTER TABLE builder
  DROP COLUMN store_dir;
