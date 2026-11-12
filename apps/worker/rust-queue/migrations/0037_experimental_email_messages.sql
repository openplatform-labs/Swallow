-- Partial reference email-message history. Does not implement the Novu
-- message API, retention policy, content storage settings or provider updates.
CREATE TABLE IF NOT EXISTS novu_rust_email_messages (
    internal_id text PRIMARY KEY,
    environment_id text NOT NULL,
    subscriber_id text NOT NULL,
    subscriber_internal_id text NOT NULL,
    notification_internal_id text NOT NULL,
    workflow_identifier text NOT NULL,
    transaction_id text NOT NULL,
    email text,
    subject text NOT NULL,
    status text NOT NULL CHECK (status IN ('sent','warning')),
    error_id text,
    error_text text,
    email_job_id uuid REFERENCES novu_queue_jobs(id),
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS novu_rust_email_messages_environment_transaction_idx
    ON novu_rust_email_messages(environment_id, transaction_id);
