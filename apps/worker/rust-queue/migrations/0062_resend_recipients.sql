-- Snapshot only hashes; old attempts cannot be reconstructed safely.
CREATE TABLE IF NOT EXISTS novu_rust_resend_recipients (
  job_id uuid NOT NULL REFERENCES novu_rust_resend_attempts(job_id) ON DELETE CASCADE,
  environment_id text NOT NULL,
  recipient_kind text NOT NULL CHECK (recipient_kind IN ('to','cc','bcc')),
  recipient_index integer NOT NULL CHECK (recipient_index >= 0),
  recipient_hash text NOT NULL,
  PRIMARY KEY (job_id, recipient_kind, recipient_index)
);
CREATE INDEX IF NOT EXISTS novu_rust_resend_attempt_provider_idx
  ON novu_rust_resend_attempts(environment_id, provider_message_id)
  WHERE provider_message_id IS NOT NULL;
