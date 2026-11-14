-- Separate provider evidence from dispatch status and Novu-compatible status.
ALTER TABLE novu_queue_jobs ADD COLUMN IF NOT EXISTS twilio_delivery_result text;
ALTER TABLE novu_rust_sms_messages ADD COLUMN IF NOT EXISTS twilio_delivery_result text;
CREATE INDEX IF NOT EXISTS novu_queue_twilio_projection_idx ON novu_queue_jobs(environment_id,id) WHERE kind='experimental_twilio_sms';
CREATE INDEX IF NOT EXISTS novu_twilio_attempt_provider_idx ON novu_rust_twilio_attempts(environment_id,provider_message_id) WHERE provider_message_id IS NOT NULL;
CREATE TABLE IF NOT EXISTS novu_rust_twilio_projection_heartbeat (
    environment_id text PRIMARY KEY,
    last_completed_at timestamptz NOT NULL
);
