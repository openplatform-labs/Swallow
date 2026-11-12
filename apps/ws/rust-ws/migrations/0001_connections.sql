-- Experimental shared connection registry. It is not a Socket.IO adapter.
CREATE TABLE IF NOT EXISTS novu_rust_ws_connections (
    id uuid PRIMARY KEY,
    environment_id text NOT NULL,
    subscriber_internal_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS novu_rust_ws_connections_subscriber_idx
    ON novu_rust_ws_connections(environment_id, subscriber_internal_id);
