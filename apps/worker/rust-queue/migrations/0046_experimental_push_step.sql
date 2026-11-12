-- Manually configured FCM step and device targets; not Novu integration management.
ALTER TABLE novu_rust_workflows ADD COLUMN IF NOT EXISTS push_title text;
ALTER TABLE novu_rust_workflows ADD COLUMN IF NOT EXISTS push_content text;
ALTER TABLE novu_rust_workflows ADD COLUMN IF NOT EXISTS push_step_active boolean NOT NULL DEFAULT true;
ALTER TABLE novu_rust_workflow_preferences ADD COLUMN IF NOT EXISTS push_enabled boolean;
CREATE TABLE IF NOT EXISTS novu_rust_fcm_devices (
    environment_id text NOT NULL,
    subscriber_id text NOT NULL,
    device_tokens text[] NOT NULL DEFAULT '{}',
    PRIMARY KEY (environment_id,subscriber_id),
    FOREIGN KEY (environment_id,subscriber_id) REFERENCES novu_rust_subscribers(environment_id,subscriber_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS novu_rust_push_step_outcomes (
    job_id uuid NOT NULL REFERENCES novu_queue_jobs(id) ON DELETE CASCADE,
    environment_id text NOT NULL,
    subscriber_id text NOT NULL,
    subscriber_internal_id text NOT NULL,
    workflow_identifier text NOT NULL,
    transaction_id text NOT NULL,
    status text NOT NULL CHECK (status IN ('queued','canceled','missing_device','failed')),
    push_job_id uuid REFERENCES novu_queue_jobs(id),
    PRIMARY KEY (job_id,subscriber_internal_id),
    CHECK ((status='queued') = (push_job_id IS NOT NULL))
);
