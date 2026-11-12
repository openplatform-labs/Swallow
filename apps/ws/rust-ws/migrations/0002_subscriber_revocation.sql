-- Best-effort, post-commit notification to connected WS instances.
-- The periodic identity check provides eventual recovery if NOTIFY is missed.
CREATE OR REPLACE FUNCTION novu_rust_notify_subscriber_deleted() RETURNS trigger AS $$
DECLARE payload text;
BEGIN
    payload := json_build_object('environment', OLD.environment_id, 'subscriber', OLD.internal_id)::text;
    IF octet_length(payload) < 8000 THEN
        PERFORM pg_notify('novu_rust_subscriber_deleted', payload);
    END IF;
    RETURN OLD;
EXCEPTION WHEN OTHERS THEN
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS novu_rust_subscriber_deleted_notify ON novu_rust_subscribers;
CREATE TRIGGER novu_rust_subscriber_deleted_notify AFTER DELETE ON novu_rust_subscribers
    FOR EACH ROW EXECUTE FUNCTION novu_rust_notify_subscriber_deleted();
