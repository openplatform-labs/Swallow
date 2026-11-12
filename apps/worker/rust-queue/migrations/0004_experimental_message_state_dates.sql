-- Upgrade for experimental in-app messages; not a migration from Novu.
ALTER TABLE novu_rust_in_app_messages
  ADD COLUMN IF NOT EXISTS first_seen_at timestamptz,
  ADD COLUMN IF NOT EXISTS last_seen_at timestamptz,
  ADD COLUMN IF NOT EXISTS last_read_at timestamptz,
  ADD COLUMN IF NOT EXISTS archived_at timestamptz;
