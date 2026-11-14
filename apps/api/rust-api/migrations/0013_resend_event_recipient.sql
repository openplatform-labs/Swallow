-- Only events with exactly one recipient can be correlated safely.
ALTER TABLE novu_rust_resend_delivery_events ADD COLUMN IF NOT EXISTS recipient_hash text;
CREATE INDEX IF NOT EXISTS novu_rust_resend_event_recipient_idx
  ON novu_rust_resend_delivery_events(environment_id, email_id, recipient_hash)
  WHERE recipient_hash IS NOT NULL;
