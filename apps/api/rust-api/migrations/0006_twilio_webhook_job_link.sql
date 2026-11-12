-- A signed job-specific callback binds evidence to a particular queue job.
-- No foreign key: API migrations can run before the experimental queue exists.
ALTER TABLE novu_rust_twilio_delivery_events ADD COLUMN IF NOT EXISTS job_id uuid;
CREATE INDEX IF NOT EXISTS novu_rust_twilio_events_job_idx
    ON novu_rust_twilio_delivery_events(environment_id, job_id, received_at)
    WHERE job_id IS NOT NULL;
