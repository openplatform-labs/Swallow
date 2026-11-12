-- Experimental context identity and in-app message scoping.
CREATE TABLE IF NOT EXISTS novu_rust_contexts (
  environment_id text NOT NULL,
  context_key text NOT NULL,
  PRIMARY KEY (environment_id, context_key)
);
ALTER TABLE novu_rust_in_app_messages
  ADD COLUMN IF NOT EXISTS context_keys text[] NOT NULL DEFAULT ARRAY[]::text[];
CREATE INDEX IF NOT EXISTS novu_rust_message_context_idx
  ON novu_rust_in_app_messages USING gin (context_keys);
