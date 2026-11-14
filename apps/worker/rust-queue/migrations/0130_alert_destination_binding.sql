-- Optional, immutable environment/channel/target association. Fingerprints are sensitive.
CREATE TABLE public.novu_rust_alert_destination_bindings (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),
 channel text NOT NULL CHECK (channel IN ('slack','email')),
 target_id text NOT NULL CHECK (octet_length(target_id) BETWEEN 1 AND 64 AND target_id ~ '^[A-Za-z0-9._-]+$'),
 fingerprint text NOT NULL CHECK (fingerprint ~ '^[0-9a-f]{64}$'),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(environment_id,channel,target_id)
);
REVOKE ALL ON public.novu_rust_alert_destination_bindings FROM PUBLIC;

CREATE FUNCTION public.novu_rust_bind_alert_destination(p_env text,p_channel text,p_target text,p_fingerprint text) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_match boolean;
BEGIN
 IF p_env IS NULL OR length(btrim(p_env))=0 OR p_channel IS NULL OR p_channel NOT IN ('slack','email')
 OR p_target IS NULL OR octet_length(p_target) NOT BETWEEN 1 AND 64 OR p_target !~ '^[A-Za-z0-9._-]+$'
 OR p_fingerprint IS NULL OR p_fingerprint !~ '^[0-9a-f]{64}$' THEN
  RAISE EXCEPTION 'invalid alert destination binding' USING ERRCODE='22023';
 END IF;
 IF NOT public.novu_rust_lock_operator_grant(p_env,'alert_send') THEN
  RAISE EXCEPTION 'alert send permission denied' USING ERRCODE='42501';
 END IF;
 IF NOT EXISTS(SELECT 1 FROM public.novu_rust_environments WHERE id=p_env) THEN RAISE EXCEPTION 'environment not found' USING ERRCODE='22023'; END IF;
 INSERT INTO public.novu_rust_alert_destination_bindings(environment_id,channel,target_id,fingerprint)
 VALUES(p_env,p_channel,p_target,p_fingerprint) ON CONFLICT DO NOTHING;
 SELECT b.fingerprint=p_fingerprint INTO v_match FROM public.novu_rust_alert_destination_bindings b
 WHERE b.environment_id=p_env AND b.channel=p_channel AND b.target_id=p_target;
 RETURN coalesce(v_match,false);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_bind_alert_destination(text,text,text,text) FROM PUBLIC;
