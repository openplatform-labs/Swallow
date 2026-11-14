-- Independently provisioned, short-lived console credentials, never API-key hashes.
CREATE TABLE IF NOT EXISTS public.novu_rust_console_credentials (
 token_hash text PRIMARY KEY CHECK (token_hash ~ '^[0-9a-f]{64}$'),
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),
 enabled boolean NOT NULL DEFAULT true,
 expires_at timestamptz NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
REVOKE ALL ON public.novu_rust_console_credentials FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_console_subscribers(p_hash text,p_name text,p_after text,p_before text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_env text; v_expires timestamptz; v_result jsonb;
BEGIN
 IF p_hash IS NULL OR p_hash !~ '^[0-9a-f]{64}$' OR (p_name IS NOT NULL AND octet_length(p_name)>512)
 OR (p_after IS NOT NULL AND (octet_length(p_after)>512 OR p_after=''))
 OR (p_before IS NOT NULL AND (octet_length(p_before)>512 OR p_before=''))
 OR (p_after IS NOT NULL AND p_before IS NOT NULL) THEN
  RAISE EXCEPTION 'invalid request' USING ERRCODE='22023';
 END IF;
 SELECT c.environment_id,c.expires_at INTO v_env,v_expires FROM public.novu_rust_console_credentials c
 WHERE c.token_hash=p_hash AND c.enabled AND c.expires_at>clock_timestamp() FOR SHARE OF c;
 IF NOT FOUND OR v_expires<=clock_timestamp() THEN RAISE EXCEPTION 'invalid credential' USING ERRCODE='42501'; END IF;
 WITH filtered AS MATERIALIZED (
   SELECT s.subscriber_id,s.profile FROM public.novu_rust_subscribers s
   WHERE s.environment_id=v_env AND (p_name IS NULL OR position(lower(p_name) in lower(trim(coalesce(s.profile->>'firstName','') || ' ' || coalesce(s.profile->>'lastName',''))))>0)
 ), candidates AS MATERIALIZED (
   SELECT * FROM filtered WHERE (p_after IS NULL OR subscriber_id COLLATE "C">p_after COLLATE "C") AND (p_before IS NULL OR subscriber_id COLLATE "C"<p_before COLLATE "C")
   ORDER BY CASE WHEN p_before IS NULL THEN subscriber_id END COLLATE "C" ASC,CASE WHEN p_before IS NOT NULL THEN subscriber_id END COLLATE "C" DESC LIMIT 20
 ), bounds AS (
   SELECT min(subscriber_id COLLATE "C") AS first_id,max(subscriber_id COLLATE "C") AS last_id FROM candidates
 )
 SELECT jsonb_build_object('data',coalesce((SELECT jsonb_agg(jsonb_build_object('subscriberId',subscriber_id,'firstName',profile->>'firstName','lastName',profile->>'lastName','email',profile->>'email') ORDER BY subscriber_id COLLATE "C") FROM candidates),'[]'::jsonb),
  'previous',CASE WHEN EXISTS(SELECT 1 FROM filtered f WHERE f.subscriber_id COLLATE "C"<b.first_id COLLATE "C") THEN b.first_id ELSE NULL END,
  'next',CASE WHEN EXISTS(SELECT 1 FROM filtered f WHERE f.subscriber_id COLLATE "C">b.last_id COLLATE "C") THEN b.last_id ELSE NULL END) INTO v_result FROM bounds b;
 RETURN v_result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_console_subscribers(text,text,text,text) FROM PUBLIC;
