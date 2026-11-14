-- Minimal authenticated evidence; no message content or recipient addresses.
CREATE TABLE IF NOT EXISTS novu_rust_resend_delivery_events (
    environment_id text NOT NULL REFERENCES novu_rust_environments(id),
    event_id text NOT NULL,
    email_id text NOT NULL,
    event_type text NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (environment_id, event_id)
);
CREATE INDEX IF NOT EXISTS novu_rust_resend_delivery_email_idx
    ON novu_rust_resend_delivery_events(environment_id, email_id);
