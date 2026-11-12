-- NULL means use the legacy single delivery timestamp. A non-NULL array
-- preserves every delivery, including an explicitly empty history.
ALTER TABLE novu_rust_in_app_messages ADD COLUMN IF NOT EXISTS delivered_history timestamptz[];
