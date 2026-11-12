-- A non-null timestamp means snoozed until an unsnooze job clears it.
-- This does not create or schedule unsnooze jobs.
ALTER TABLE novu_rust_in_app_messages ADD COLUMN IF NOT EXISTS snoozed_until timestamptz;
