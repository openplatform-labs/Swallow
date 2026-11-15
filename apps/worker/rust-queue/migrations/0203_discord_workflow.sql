-- Subscriber endpoints refer to a secret held by the Discord worker, never a URL.
CREATE TABLE IF NOT EXISTS novu_rust_discord_workflow_endpoints (
    environment_id text NOT NULL,
    identifier text NOT NULL CHECK (length(identifier) BETWEEN 1 AND 255),
    internal_id uuid NOT NULL DEFAULT gen_random_uuid(),
    revision uuid NOT NULL DEFAULT gen_random_uuid(),
    subscriber_id text NOT NULL,
    subscriber_internal_id text NOT NULL,
    destination_id text NOT NULL,
    context_keys text[] NOT NULL DEFAULT '{}',
    PRIMARY KEY(environment_id,identifier),
    UNIQUE(environment_id,internal_id),
    FOREIGN KEY(environment_id,subscriber_id,subscriber_internal_id)
      REFERENCES novu_rust_subscribers(environment_id,subscriber_id,internal_id) ON DELETE CASCADE,
    FOREIGN KEY(environment_id,destination_id)
      REFERENCES novu_rust_discord_destinations(environment_id,destination_id),
    CHECK (array_position(context_keys,NULL) IS NULL)
);
CREATE OR REPLACE FUNCTION novu_rust_discord_endpoint_revision() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN NEW.revision := gen_random_uuid(); RETURN NEW; END $$;
DROP TRIGGER IF EXISTS novu_rust_discord_endpoint_revision_trigger ON novu_rust_discord_workflow_endpoints;
CREATE TRIGGER novu_rust_discord_endpoint_revision_trigger BEFORE UPDATE ON novu_rust_discord_workflow_endpoints
FOR EACH ROW EXECUTE FUNCTION novu_rust_discord_endpoint_revision();
CREATE INDEX IF NOT EXISTS novu_rust_discord_endpoint_subscriber_idx
    ON novu_rust_discord_workflow_endpoints(environment_id,subscriber_internal_id,internal_id);
CREATE INDEX IF NOT EXISTS novu_queue_discord_workflow_pending_idx
    ON novu_queue_jobs(environment_id,(payload->>'destination'),available_at,created_at,id)
    WHERE kind='experimental_discord_workflow' AND status='pending';
CREATE INDEX IF NOT EXISTS novu_queue_discord_workflow_running_idx
    ON novu_queue_jobs(environment_id,(payload->>'destination'),lease_until,id)
    WHERE kind='experimental_discord_workflow' AND status='running';
