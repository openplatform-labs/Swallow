-- Retain SendGrid request identities through operator retries.
CREATE TABLE IF NOT EXISTS novu_rust_sendgrid_dispatch_ids (
    id uuid PRIMARY KEY,
    job_id uuid NOT NULL REFERENCES novu_queue_jobs(id) ON DELETE CASCADE,
    environment_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS novu_rust_sendgrid_dispatch_ids_job_idx
    ON novu_rust_sendgrid_dispatch_ids(environment_id, job_id, created_at);
ALTER TABLE novu_rust_sendgrid_attempts ADD COLUMN IF NOT EXISTS dispatch_id uuid;
