-- Experimental per-recipient notification records. They retain the original
-- subscriber internal ID after subscriber deletion, as reference history does.
CREATE TABLE IF NOT EXISTS novu_rust_notifications (
  internal_id text PRIMARY KEY DEFAULT novu_rust_new_objectid(),
  job_id uuid NOT NULL,
  environment_id text NOT NULL,
  organization_id text,
  subscriber_id text NOT NULL,
  subscriber_internal_id text NOT NULL,
  workflow_identifier text NOT NULL,
  workflow_internal_id text,
  transaction_id text NOT NULL,
  recipient jsonb NOT NULL,
  payload jsonb NOT NULL,
  channels text[] NOT NULL,
  tags jsonb NOT NULL,
  severity text NOT NULL,
  critical boolean NOT NULL DEFAULT false,
  context_keys text[] NOT NULL DEFAULT ARRAY[]::text[],
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(job_id, subscriber_internal_id)
);
CREATE INDEX IF NOT EXISTS novu_rust_notifications_subscriber_idx
  ON novu_rust_notifications(environment_id, subscriber_internal_id, created_at);
ALTER TABLE novu_rust_in_app_messages ADD COLUMN IF NOT EXISTS notification_internal_id text;
