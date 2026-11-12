-- Partial SMS history; not the full Novu message API or retention model.
CREATE TABLE IF NOT EXISTS novu_rust_sms_messages (
    internal_id text PRIMARY KEY,
    environment_id text NOT NULL,
    subscriber_id text NOT NULL,
    subscriber_internal_id text NOT NULL,
    notification_internal_id text NOT NULL,
    workflow_identifier text NOT NULL,
    transaction_id text NOT NULL,
    phone text,
    content text,
    status text NOT NULL CHECK (status IN ('sent','warning','error')),
    error_id text,
    error_text text,
    identifier text,
    sms_job_id uuid REFERENCES novu_queue_jobs(id),
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS novu_rust_sms_messages_environment_transaction_idx
    ON novu_rust_sms_messages(environment_id,transaction_id);
