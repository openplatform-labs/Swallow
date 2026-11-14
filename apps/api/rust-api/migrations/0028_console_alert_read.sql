-- Explicitly provisioned alert visibility; existing console credentials have no grant.
CREATE TABLE public.novu_rust_console_alert_grants (
 credential_id uuid PRIMARY KEY REFERENCES public.novu_rust_console_credentials(credential_id) ON DELETE CASCADE
);
REVOKE ALL ON public.novu_rust_console_alert_grants FROM PUBLIC;

CREATE FUNCTION public.novu_rust_console_alerts(p_hash text) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_env text; v_id uuid; v_expires timestamptz; v_result jsonb;
BEGIN
 IF p_hash IS NULL OR p_hash !~ '^[0-9a-f]{64}$' THEN
   RAISE EXCEPTION 'invalid credential' USING ERRCODE='42501';
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
   SELECT s.channel,s.target_id,CASE WHEN s.state='dispatching' AND s.lease_until<=clock_timestamp() THEN 'expired' ELSE s.state END AS state,s.last_started_at
   FROM public.novu_rust_alert_delivery_slots s WHERE s.environment_id=v_env ORDER BY s.channel,s.target_id LIMIT 101
 )
 SELECT jsonb_build_object('data',coalesce((SELECT jsonb_agg(jsonb_build_object('channel',channel,'targetId',target_id,'state',state,'lastStartedAt',last_started_at) ORDER BY channel,target_id) FROM (SELECT * FROM slots ORDER BY channel,target_id LIMIT 100) x),'[]'::jsonb),'hasMore',(SELECT count(*)>100 FROM slots)) INTO v_result;
 RETURN v_result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_console_alerts(text) FROM PUBLIC;
