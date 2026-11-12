-- Upgrade for the experimental schema only; not a migration from Novu.
ALTER TABLE novu_rust_in_app_messages
  ADD COLUMN IF NOT EXISTS is_read boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS is_seen boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS is_archived boolean NOT NULL DEFAULT false;
