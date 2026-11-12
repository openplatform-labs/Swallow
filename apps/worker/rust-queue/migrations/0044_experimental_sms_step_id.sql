-- Step-scoped provider overrides require the configured Novu step identifier.
ALTER TABLE novu_rust_workflows ADD COLUMN IF NOT EXISTS sms_step_id text;
