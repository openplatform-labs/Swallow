-- Foundation for non-secret Slack channel/user endpoints, not the full Novu API.
-- The triple foreign key fences a deleted/recreated external subscriber ID.
CREATE UNIQUE INDEX IF NOT EXISTS novu_rust_subscribers_identity_idx
    ON novu_rust_subscribers(environment_id,subscriber_id,internal_id);
CREATE TABLE IF NOT EXISTS novu_rust_slack_channel_endpoints (
    environment_id text NOT NULL,
    identifier text NOT NULL CHECK (length(identifier) BETWEEN 1 AND 255),
    internal_id uuid NOT NULL DEFAULT gen_random_uuid(),
    subscriber_id text NOT NULL,
    subscriber_internal_id text NOT NULL,
    integration_identifier text NOT NULL CHECK (length(integration_identifier) BETWEEN 1 AND 255),
    connection_identifier text CHECK (connection_identifier IS NULL OR length(connection_identifier) BETWEEN 1 AND 255),
    context_keys text[] NOT NULL DEFAULT '{}',
    endpoint_type text NOT NULL CHECK (endpoint_type IN ('slack_channel','slack_user')),
    endpoint jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (environment_id,identifier),
    UNIQUE (environment_id,internal_id),
    FOREIGN KEY (environment_id,subscriber_id,subscriber_internal_id)
      REFERENCES novu_rust_subscribers(environment_id,subscriber_id,internal_id) ON DELETE CASCADE,
    CHECK (array_position(context_keys,NULL) IS NULL),
    CHECK (
      (endpoint_type='slack_channel' AND jsonb_typeof(endpoint)='object'
       AND endpoint ? 'channelId' AND jsonb_typeof(endpoint->'channelId')='string'
       AND length(endpoint->>'channelId') BETWEEN 1 AND 255 AND endpoint - 'channelId'='{}'::jsonb)
      OR
      (endpoint_type='slack_user' AND jsonb_typeof(endpoint)='object'
       AND endpoint ? 'userId' AND jsonb_typeof(endpoint->'userId')='string'
       AND length(endpoint->>'userId') BETWEEN 1 AND 255 AND endpoint - 'userId'='{}'::jsonb)
    )
);
CREATE INDEX IF NOT EXISTS novu_rust_slack_channel_endpoints_subscriber_idx
    ON novu_rust_slack_channel_endpoints(environment_id,subscriber_internal_id,created_at,identifier);
