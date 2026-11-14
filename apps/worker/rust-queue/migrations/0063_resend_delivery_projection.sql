-- Delivery evidence is distinct from queue dispatch state and Novu's sent status.
ALTER TABLE novu_queue_jobs ADD COLUMN IF NOT EXISTS resend_delivery_result text;
ALTER TABLE novu_rust_email_messages ADD COLUMN IF NOT EXISTS resend_delivery_result text;
CREATE INDEX IF NOT EXISTS novu_queue_resend_projection_idx ON novu_queue_jobs(environment_id,id) WHERE kind='experimental_resend_email';
