-- Your SQL goes here

ALTER TABLE picked_eval_job
  ADD COLUMN builder_id uuid NOT NULL,
  ADD CONSTRAINT builder_id FOREIGN KEY ( builder_id ) REFERENCES builder ( id );

ALTER TABLE concluded_eval_job
  ADD COLUMN builder_id uuid NOT NULL,
  ADD CONSTRAINT builder_id FOREIGN KEY ( builder_id ) REFERENCES builder ( id );
