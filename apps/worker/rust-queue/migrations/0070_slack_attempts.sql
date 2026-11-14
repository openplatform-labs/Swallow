-- Experimental app-based chat attempts; tokens are not stored here.
CREATE TABLE IF NOT EXISTS novu_rust_slack_attempts (
    job_id uuid PRIMARY KEY REFERENCES novu_queue_jobs(id) ON DELETE CASCADE,
    environment_id text NOT NULL,
    state text NOT NULL CHECK (state IN ('dispatching','accepted','rejected','unknown')),
    http_status integer,
    provider_message_id text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS novu_rust_slack_attempts_state_idx ON novu_rust_slack_attempts(environment_id,state,updated_at);
CREATE TABLE IF NOT EXISTS novu_rust_slack_resolutions (
    id uuid PRIMARY KEY,
    environment_id text NOT NULL,
    job_id uuid NOT NULL,
    previous_state text NOT NULL CHECK (previous_state IN ('unknown','rejected')),
    decision text NOT NULL CHECK (decision IN ('retry','accepted')),
    operator_name text NOT NULL,
    reason text NOT NULL,
    database_user text NOT NULL DEFAULT current_user,
    database_session_user text NOT NULL DEFAULT session_user,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
