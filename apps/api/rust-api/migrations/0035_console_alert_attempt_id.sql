-- Preserve the paginated function and its grants while exposing the current non-secret attempt ID.
CREATE OR REPLACE FUNCTION public.novu_rust_console_alerts_page(p_hash text,p_after_channel text,p_after_target text) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_env text; v_id uuid; v_expires timestamptz; v_result jsonb;
BEGIN
 IF p_hash IS NULL OR p_hash !~ '^[0-9a-f]{64}$' THEN
   RAISE EXCEPTION 'invalid credential' USING ERRCODE='42501';
 END IF;
 IF (p_after_channel IS NULL) <> (p_after_target IS NULL) OR (p_after_channel IS NOT NULL AND (p_after_channel NOT IN ('email','slack') OR p_after_target !~ '^[A-Za-z0-9._-]{1,64}$')) THEN
   RAISE EXCEPTION 'invalid cursor' USING ERRCODE='22023';
 END IF;
 SELECT c.environment_id,c.credential_id,c.expires_at INTO v_env,v_id,v_expires
 FROM public.novu_rust_console_credentials c
 WHERE c.token_hash=p_hash AND c.enabled AND c.expires_at>clock_timestamp() FOR SHARE OF c;
 IF NOT FOUND OR v_expires<=clock_timestamp() THEN
   RAISE EXCEPTION 'invalid credential' USING ERRCODE='42501';
 END IF;
 PERFORM 1 FROM public.novu_rust_console_alert_grants g WHERE g.credential_id=v_id FOR SHARE OF g;
 IF NOT FOUND OR v_expires<=clock_timestamp() THEN
   RAISE EXCEPTION 'invalid credential' USING ERRCODE='42501';
 END IF;
 WITH slots AS MATERIALIZED (
   SELECT s.channel,s.target_id,CASE WHEN s.state='dispatching' AND s.lease_until<=clock_timestamp() THEN 'expired' ELSE s.state END AS state,s.last_started_at,s.attempt_id
   FROM public.novu_rust_alert_delivery_slots s WHERE s.environment_id=v_env AND (p_after_channel IS NULL OR (s.channel COLLATE "C",s.target_id COLLATE "C") > (p_after_channel COLLATE "C",p_after_target COLLATE "C")) ORDER BY s.channel COLLATE "C",s.target_id COLLATE "C" LIMIT 101
 )
 SELECT jsonb_build_object('data',coalesce((SELECT jsonb_agg(jsonb_build_object('channel',channel,'targetId',target_id,'state',state,'lastStartedAt',last_started_at,'attemptId',attempt_id) ORDER BY channel COLLATE "C",target_id COLLATE "C") FROM (SELECT * FROM slots ORDER BY channel COLLATE "C",target_id COLLATE "C" LIMIT 100) x),'[]'::jsonb),'hasMore',(SELECT count(*)>100 FROM slots),'next',CASE WHEN (SELECT count(*)>100 FROM slots) THEN (SELECT jsonb_build_object('channel',channel,'targetId',target_id) FROM (SELECT * FROM slots ORDER BY channel COLLATE "C",target_id COLLATE "C" LIMIT 100) p ORDER BY channel COLLATE "C" DESC,target_id COLLATE "C" DESC LIMIT 1) ELSE NULL END) INTO v_result;
 RETURN v_result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_console_alerts_page(text,text,text) FROM PUBLIC;
