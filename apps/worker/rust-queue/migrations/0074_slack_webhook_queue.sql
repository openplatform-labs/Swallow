-- Index used to select only the configured destination for an environment.
CREATE TABLE IF NOT EXISTS novu_rust_slack_webhook_destinations (
    environment_id text NOT NULL REFERENCES novu_rust_environments(id),
    destination_id text NOT NULL,
    endpoint_hash text NOT NULL,
    PRIMARY KEY (environment_id,destination_id)
);
CREATE INDEX IF NOT EXISTS novu_queue_slack_webhook_destination_idx
    ON novu_queue_jobs(environment_id,(payload->>'destination'),available_at,created_at,id)
    WHERE kind='experimental_slack_webhook' AND status='pending';
CREATE INDEX IF NOT EXISTS novu_queue_slack_webhook_recovery_idx
    ON novu_queue_jobs(environment_id,(payload->>'destination'),lease_until,id)
    WHERE kind='experimental_slack_webhook' AND status='running';
