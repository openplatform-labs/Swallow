-- Distinguish MongoDB's absent dates from explicit null after state changes.
ALTER TABLE novu_rust_in_app_messages
  ADD COLUMN IF NOT EXISTS read_at_present boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS archived_at_present boolean NOT NULL DEFAULT false;
UPDATE novu_rust_in_app_messages SET read_at_present = true WHERE last_read_at IS NOT NULL AND NOT read_at_present;
UPDATE novu_rust_in_app_messages SET archived_at_present = true WHERE archived_at IS NOT NULL AND NOT archived_at_present;
