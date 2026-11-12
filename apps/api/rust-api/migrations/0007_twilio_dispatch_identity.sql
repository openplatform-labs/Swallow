-- Older and manually configured callbacks may not identify an individual dispatch.
ALTER TABLE novu_rust_twilio_delivery_events ADD COLUMN IF NOT EXISTS dispatch_id uuid;
