-- Experimental single Slack workflow step and durable endpoint fanout.
ALTER TABLE novu_rust_workflows ADD COLUMN IF NOT EXISTS chat_content text;
ALTER TABLE novu_rust_workflows ADD COLUMN IF NOT EXISTS chat_step_active boolean NOT NULL DEFAULT true;
ALTER TABLE novu_rust_workflow_preferences ADD COLUMN IF NOT EXISTS chat_enabled boolean;
CREATE TABLE IF NOT EXISTS novu_rust_slack_workflow_fanout (
    parent_job_id uuid NOT NULL REFERENCES novu_queue_jobs(id) ON DELETE CASCADE,
    environment_id text NOT NULL,
    endpoint_internal_id uuid NOT NULL,
    delivery_job_id uuid NOT NULL UNIQUE REFERENCES novu_queue_jobs(id),
    PRIMARY KEY(parent_job_id,endpoint_internal_id)
);
CREATE INDEX IF NOT EXISTS novu_rust_slack_workflow_delivery_idx
    ON novu_queue_jobs(environment_id,kind,status,available_at,created_at,id)
    WHERE kind IN ('experimental_slack_workflow_fanout','experimental_slack_workflow_delivery');
-- One credential-map fingerprint per environment avoids mixed worker routing.
CREATE TABLE IF NOT EXISTS novu_rust_slack_workflow_worker_config (
    environment_id text PRIMARY KEY REFERENCES novu_rust_environments(id),
    credential_map_hash text NOT NULL
);
