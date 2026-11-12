-- The original WebSocketsQueueService drops enqueue errors. Isolate failures
-- inside a subtransaction so they do not roll back the caller's state change.
CREATE OR REPLACE FUNCTION novu_rust_emit_ws_event(
    p_environment text, p_subscriber text, p_context text[], p_event text, p_data jsonb
) RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO novu_rust_ws_events(environment_id,subscriber_internal_id,context_keys,event,data)
    VALUES (p_environment,p_subscriber,COALESCE(p_context,ARRAY[]::text[]),p_event,p_data);
    RETURN true;
EXCEPTION WHEN OTHERS THEN
    RETURN false;
END
$$;
