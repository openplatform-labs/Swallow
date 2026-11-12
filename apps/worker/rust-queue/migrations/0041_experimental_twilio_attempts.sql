-- Dispatching means that external acceptance may already have occurred.
CREATE TABLE IF NOT EXISTS novu_rust_twilio_attempts (
    job_id uuid PRIMARY KEY REFERENCES novu_queue_jobs(id) ON DELETE CASCADE,
    environment_id text NOT NULL,
    state text NOT NULL CHECK (state IN ('dispatching','accepted','rejected','unknown')),
    http_status integer,
    provider_message_id text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
