-- Explicitly configured single email step; this is not Novu's workflow model.
ALTER TABLE novu_rust_workflows ADD COLUMN IF NOT EXISTS email_subject text;
ALTER TABLE novu_rust_workflows ADD COLUMN IF NOT EXISTS email_html text;
ALTER TABLE novu_rust_workflows ADD COLUMN IF NOT EXISTS email_step_active boolean NOT NULL DEFAULT true;
ALTER TABLE novu_rust_workflow_preferences ADD COLUMN IF NOT EXISTS email_enabled boolean;

CREATE TABLE IF NOT EXISTS novu_rust_email_step_outcomes (
    job_id uuid NOT NULL REFERENCES novu_queue_jobs(id) ON DELETE CASCADE,
    environment_id text NOT NULL,
    subscriber_id text NOT NULL,
    subscriber_internal_id text NOT NULL,
    workflow_identifier text NOT NULL,
    transaction_id text NOT NULL,
    status text NOT NULL CHECK (status IN ('queued','canceled','missing_email')),
    email_job_id uuid REFERENCES novu_queue_jobs(id),
    PRIMARY KEY (job_id, subscriber_internal_id),
    CHECK ((status = 'queued') = (email_job_id IS NOT NULL))
);
