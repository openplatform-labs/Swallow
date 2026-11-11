-- Prototype schema; not a migration from Novu or its full subscriber/message model.
CREATE TABLE IF NOT EXISTS novu_rust_subscribers (
  environment_id text NOT NULL,
  subscriber_id text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (environment_id, subscriber_id)
);
CREATE TABLE IF NOT EXISTS novu_rust_in_app_messages (
  id uuid PRIMARY KEY,
  environment_id text NOT NULL,
  subscriber_id text NOT NULL,
  workflow_identifier text NOT NULL,
  transaction_id text NOT NULL,
  content text NOT NULL,
  payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (environment_id, subscriber_id) REFERENCES novu_rust_subscribers(environment_id, subscriber_id),
  UNIQUE (environment_id, subscriber_id, workflow_identifier, transaction_id)
);
