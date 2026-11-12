-- Novu's preference documents can outlive subscriptions removed by legacy v1
-- and topic deletion; therefore this table deliberately has no subscription FK.
-- V2 subscription deletion and subscriber deletion remove corresponding preferences.
CREATE TABLE IF NOT EXISTS novu_rust_subscription_preferences (
  environment_id text NOT NULL,
  subscriber_internal_id text NOT NULL,
  subscription_internal_id text NOT NULL,
  workflow_internal_id text NOT NULL,
  context_keys_hash text NOT NULL DEFAULT 'DEFAULT_CONTEXT',
  context_keys text[],
  preferences jsonb NOT NULL CHECK (jsonb_typeof(preferences)='object'),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (environment_id,subscriber_internal_id,subscription_internal_id,workflow_internal_id,context_keys_hash)
);
CREATE INDEX IF NOT EXISTS novu_rust_subscription_preferences_delete_idx
  ON novu_rust_subscription_preferences(environment_id,subscription_internal_id);
