-- Public verification keys and minimal authenticated delivery evidence.
CREATE TABLE IF NOT EXISTS novu_rust_sendgrid_webhook_keys (
    environment_id text NOT NULL REFERENCES novu_rust_environments(id),
    key_id text NOT NULL,
    public_key_base64 text NOT NULL CHECK (public_key_base64 ~ '^[A-Za-z0-9+/]+={0,2}$'),
    active boolean NOT NULL DEFAULT true,
    PRIMARY KEY (environment_id,key_id)
);
-- A signed body does not include our environment path. Do not trust one key
-- for multiple environments, or the same request could be replayed across them.
CREATE UNIQUE INDEX IF NOT EXISTS novu_rust_sendgrid_webhook_keys_active_key_idx
    ON novu_rust_sendgrid_webhook_keys(public_key_base64) WHERE active;
CREATE TABLE IF NOT EXISTS novu_rust_sendgrid_delivery_events (
    environment_id text NOT NULL,
    event_id text NOT NULL,
    message_internal_id text NOT NULL,
    event_type text NOT NULL CHECK (event_type IN ('delivered','bounce','dropped','blocked','deferred','processed')),
    event_timestamp bigint NOT NULL,
    recipient_hash text NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (environment_id,event_id)
);
CREATE INDEX IF NOT EXISTS novu_rust_sendgrid_events_message_idx ON novu_rust_sendgrid_delivery_events(environment_id,message_internal_id,event_timestamp);
