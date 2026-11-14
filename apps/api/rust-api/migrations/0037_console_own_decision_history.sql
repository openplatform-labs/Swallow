-- Targeted reconciliation without exposing other credentials' audit records.
CREATE INDEX novu_rust_console_decision_history_idx ON public.novu_rust_console_alert_decision_audit(environment_id,credential_id,channel,target_id,attempt_id,id DESC);
CREATE FUNCTION public.novu_rust_console_decision_history(p_hash text,p_channel text,p_target text,p_attempt uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_env text; v_id uuid; v_expires timestamptz; v_result jsonb;
BEGIN
 IF p_hash IS NULL OR p_hash !~ '^[0-9a-f]{64}$' THEN RAISE EXCEPTION 'invalid credential' USING ERRCODE='42501'; END IF;
 IF p_channel IS NULL OR p_channel NOT IN ('slack','email') OR p_target IS NULL OR p_target !~ '^[A-Za-z0-9._-]{1,64}$' OR p_attempt IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 SELECT c.environment_id,c.credential_id,c.expires_at INTO v_env,v_id,v_expires FROM public.novu_rust_console_credentials c
 WHERE c.token_hash=p_hash AND c.enabled AND c.expires_at>clock_timestamp() FOR SHARE OF c;
 IF NOT FOUND OR v_expires<=clock_timestamp() THEN RAISE EXCEPTION 'invalid credential' USING ERRCODE='42501'; END IF;
 PERFORM 1 FROM public.novu_rust_console_alert_decision_grants g WHERE g.credential_id=v_id ORDER BY g.decision FOR SHARE OF g;
 IF NOT FOUND OR v_expires<=clock_timestamp() THEN RAISE EXCEPTION 'invalid credential' USING ERRCODE='42501'; END IF;
 SELECT coalesce(jsonb_agg(jsonb_build_object('id',id::text,'decision',decision,'reason',reason,'createdAt',created_at) ORDER BY id DESC),'[]'::jsonb)
 INTO v_result FROM (SELECT a.id,a.decision,a.reason,a.created_at FROM public.novu_rust_console_alert_decision_audit a
 WHERE a.environment_id=v_env AND a.credential_id=v_id AND a.channel=p_channel AND a.target_id=p_target AND a.attempt_id=p_attempt ORDER BY a.id DESC LIMIT 20) q;
 RETURN jsonb_build_object('data',v_result);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_console_decision_history(text,text,text,uuid) FROM PUBLIC;
