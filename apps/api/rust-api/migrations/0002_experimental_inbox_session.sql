-- Explicit experimental Inbox configuration; API key secrets are never stored here.
CREATE TABLE IF NOT EXISTS novu_rust_inbox_environments (
  environment_id text PRIMARY KEY REFERENCES novu_rust_environments(id),
  application_identifier text UNIQUE NOT NULL,
  in_app_active boolean NOT NULL,
  hmac_required boolean NOT NULL,
  is_development boolean NOT NULL,
  remove_novu_branding boolean NOT NULL,
  max_snooze_duration_hours integer NOT NULL CHECK (max_snooze_duration_hours >= 0)
);
