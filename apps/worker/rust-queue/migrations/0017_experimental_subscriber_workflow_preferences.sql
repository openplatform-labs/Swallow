CREATE TABLE IF NOT EXISTS novu_rust_subscriber_workflow_preferences (
  environment_id text NOT NULL,
  subscriber_id text NOT NULL,
  workflow_identifier text NOT NULL,
  channels jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(channels)='object'),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(environment_id,subscriber_id,workflow_identifier),
  FOREIGN KEY(environment_id,subscriber_id) REFERENCES novu_rust_subscribers(environment_id,subscriber_id) ON DELETE CASCADE,
  FOREIGN KEY(environment_id,workflow_identifier) REFERENCES novu_rust_workflows(environment_id,identifier) ON DELETE CASCADE
);
