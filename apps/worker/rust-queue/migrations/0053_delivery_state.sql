-- Separate local dispatch knowledge from Novu-compatible message status.
-- Existing rows without an attempt are ambiguous and must not be labelled delivered.
ALTER TABLE novu_rust_email_messages ADD COLUMN IF NOT EXISTS delivery_state text NOT NULL DEFAULT 'unknown';
ALTER TABLE novu_rust_sms_messages ADD COLUMN IF NOT EXISTS delivery_state text NOT NULL DEFAULT 'unknown';
ALTER TABLE novu_rust_push_messages ADD COLUMN IF NOT EXISTS delivery_state text NOT NULL DEFAULT 'unknown';
UPDATE novu_rust_email_messages m SET delivery_state=a.state FROM novu_rust_sendgrid_attempts a WHERE a.environment_id=m.environment_id AND a.job_id=m.email_job_id;
UPDATE novu_rust_sms_messages m SET delivery_state=a.state FROM novu_rust_twilio_attempts a WHERE a.environment_id=m.environment_id AND a.job_id=m.sms_job_id;
UPDATE novu_rust_push_messages m SET delivery_state=a.state FROM novu_rust_fcm_attempts a JOIN novu_queue_jobs j ON j.id=a.job_id WHERE a.environment_id=m.environment_id AND a.job_id=m.push_job_id AND a.target_index=m.target_index AND COALESCE((j.payload->'prepared_context'->'target_metadata'->a.target_index->>'group_size')::integer,1)=1;
ALTER TABLE novu_rust_email_messages ALTER COLUMN delivery_state SET DEFAULT 'not_attempted';
ALTER TABLE novu_rust_sms_messages ALTER COLUMN delivery_state SET DEFAULT 'not_attempted';
ALTER TABLE novu_rust_push_messages ALTER COLUMN delivery_state SET DEFAULT 'not_attempted';
ALTER TABLE novu_rust_email_messages DROP CONSTRAINT IF EXISTS novu_rust_email_delivery_state_check;
ALTER TABLE novu_rust_email_messages ADD CONSTRAINT novu_rust_email_delivery_state_check CHECK (delivery_state IN ('not_attempted','dispatching','in_progress','accepted','rejected','unknown'));
ALTER TABLE novu_rust_sms_messages DROP CONSTRAINT IF EXISTS novu_rust_sms_delivery_state_check;
ALTER TABLE novu_rust_sms_messages ADD CONSTRAINT novu_rust_sms_delivery_state_check CHECK (delivery_state IN ('not_attempted','dispatching','in_progress','accepted','rejected','unknown'));
ALTER TABLE novu_rust_push_messages DROP CONSTRAINT IF EXISTS novu_rust_push_delivery_state_check;
ALTER TABLE novu_rust_push_messages ADD CONSTRAINT novu_rust_push_delivery_state_check CHECK (delivery_state IN ('not_attempted','dispatching','in_progress','accepted','rejected','unknown'));
