-- Partial push history. This is not the full Novu message API or retention model.
CREATE TABLE IF NOT EXISTS novu_rust_push_messages (
    internal_id text PRIMARY KEY,
    environment_id text NOT NULL,
    subscriber_id text NOT NULL,
    subscriber_internal_id text NOT NULL,
    notification_internal_id text NOT NULL,
    workflow_identifier text NOT NULL,
    transaction_id text NOT NULL,
    device_tokens text[] NOT NULL,
    title text NOT NULL,
    content text,
    status text NOT NULL CHECK (status IN ('sent','error')),
    error_id text,
    error_text text,
    identifier text,
    push_job_id uuid NOT NULL REFERENCES novu_queue_jobs(id),
    target_index integer NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (push_job_id,target_index)
);
CREATE INDEX IF NOT EXISTS novu_rust_push_messages_environment_transaction_idx ON novu_rust_push_messages(environment_id,transaction_id);
