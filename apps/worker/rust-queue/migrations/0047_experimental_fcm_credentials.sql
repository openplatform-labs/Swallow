-- Fixture integration identity only; provider secrets are never stored here.
CREATE TABLE IF NOT EXISTS novu_rust_fcm_integrations (
    environment_id text PRIMARY KEY,
    internal_id text NOT NULL,
    identifier text,
    active boolean NOT NULL DEFAULT true
);
ALTER TABLE novu_rust_fcm_devices ADD COLUMN IF NOT EXISTS integration_internal_id text;
