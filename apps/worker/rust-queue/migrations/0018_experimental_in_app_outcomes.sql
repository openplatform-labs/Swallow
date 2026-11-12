-- Per-recipient results are separate from the batch queue job. Historical
-- results survive subscriber deletion, as Novu's jobs do.
CREATE TABLE IF NOT EXISTS novu_rust_in_app_step_outcomes (
  job_id uuid NOT NULL REFERENCES novu_queue_jobs(id) ON DELETE CASCADE,
  environment_id text NOT NULL,
  subscriber_id text NOT NULL,
  subscriber_internal_id text NOT NULL,
  workflow_identifier text NOT NULL,
  transaction_id text NOT NULL,
  status text NOT NULL CHECK (status IN ('completed', 'canceled')),
  detail text,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (job_id, subscriber_internal_id),
  CHECK ((status = 'canceled' AND detail IS NOT NULL AND detail = 'preference') OR (status = 'completed' AND detail IS NULL))
);
