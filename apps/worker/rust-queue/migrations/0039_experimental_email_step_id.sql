-- Identifies the configured experimental email step for trigger overrides.
ALTER TABLE novu_rust_workflows ADD COLUMN IF NOT EXISTS email_step_id text;
