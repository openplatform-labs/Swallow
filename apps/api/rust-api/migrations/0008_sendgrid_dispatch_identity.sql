-- Existing events and events without an authenticated matching dispatch stay unknown.
ALTER TABLE novu_rust_sendgrid_delivery_events ADD COLUMN IF NOT EXISTS dispatch_id uuid;
