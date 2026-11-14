-- Old events cannot be retroactively fingerprinted because their payload was not saved.
ALTER TABLE novu_rust_resend_delivery_events ADD COLUMN IF NOT EXISTS payload_fingerprint bytea;
