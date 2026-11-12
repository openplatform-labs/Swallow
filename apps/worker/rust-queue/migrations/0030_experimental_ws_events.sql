-- Small notification payloads carry only this event ID. Event bodies remain in
-- PostgreSQL so that LISTEN/NOTIFY's payload limit does not truncate them.
CREATE TABLE IF NOT EXISTS novu_rust_ws_events (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    environment_id text NOT NULL,
    subscriber_internal_id text NOT NULL,
    context_keys text[] NOT NULL DEFAULT ARRAY[]::text[],
    event text NOT NULL,
    data jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE OR REPLACE FUNCTION novu_rust_notify_ws_event() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('novu_rust_ws_events', NEW.id::text);
    RETURN NEW;
END
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS novu_rust_ws_event_notify ON novu_rust_ws_events;
CREATE TRIGGER novu_rust_ws_event_notify AFTER INSERT ON novu_rust_ws_events
    FOR EACH ROW EXECUTE FUNCTION novu_rust_notify_ws_event();
