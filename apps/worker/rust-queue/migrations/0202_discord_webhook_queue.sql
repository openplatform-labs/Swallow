-- Destination IDs are environment scoped and permanently bound to one endpoint.
CREATE TABLE IF NOT EXISTS novu_rust_discord_destinations (
    environment_id text NOT NULL REFERENCES novu_rust_environments(id),
    destination_id text NOT NULL,
    endpoint_hash text NOT NULL,
    PRIMARY KEY (environment_id, destination_id)
);
CREATE TABLE IF NOT EXISTS novu_rust_discord_attempts (
    job_id uuid PRIMARY KEY REFERENCES novu_queue_jobs(id),
    environment_id text NOT NULL REFERENCES novu_rust_environments(id),
    state text NOT NULL CHECK (state IN ('dispatching','accepted','rejected','unknown')),
    provider_message_id text,
    provider_timestamp text,
    http_status integer,
    retry_after_seconds integer,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS novu_queue_discord_pending_idx
    ON novu_queue_jobs(environment_id,(payload->>'destination'),available_at,created_at,id)
    WHERE kind='experimental_discord_webhook' AND status='pending';
CREATE INDEX IF NOT EXISTS novu_queue_discord_running_idx
    ON novu_queue_jobs(environment_id,(payload->>'destination'),lease_until,id)
    WHERE kind='experimental_discord_webhook' AND status='running';
