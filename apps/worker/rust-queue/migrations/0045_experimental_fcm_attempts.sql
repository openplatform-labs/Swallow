-- Per-target state makes partial multicast progress durable. Dispatching means
-- the external outcome may be unknown and must not be retried automatically.
CREATE TABLE IF NOT EXISTS novu_rust_fcm_attempts (
    job_id uuid NOT NULL REFERENCES novu_queue_jobs(id) ON DELETE CASCADE,
    environment_id text NOT NULL,
    target_index integer NOT NULL CHECK (target_index >= 0),
    state text NOT NULL CHECK (state IN ('dispatching','accepted','rejected','unknown')),
    http_status integer,
    provider_message_id text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id,target_index)
);
