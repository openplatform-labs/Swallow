-- Minimal authenticated Twilio message status evidence; no phone or raw body.
CREATE TABLE IF NOT EXISTS novu_rust_twilio_delivery_events (
    environment_id text NOT NULL REFERENCES novu_rust_environments(id),
    account_sid text NOT NULL,
    message_sid text NOT NULL,
    status text NOT NULL,
    error_code text NOT NULL DEFAULT '',
    received_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (environment_id, account_sid, message_sid, status, error_code)
);
CREATE INDEX IF NOT EXISTS novu_rust_twilio_delivery_events_message_idx
    ON novu_rust_twilio_delivery_events(environment_id, message_sid, received_at);
