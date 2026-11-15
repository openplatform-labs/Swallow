-- Optional immutable binding of a destination to one shared-bit check.
CREATE TABLE public.novu_rust_alert_shared_check_bindings (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id) ON DELETE CASCADE,
 channel text NOT NULL CHECK (channel IN ('slack','email')),
 target_id text NOT NULL CHECK (octet_length(target_id) BETWEEN 1 AND 64 AND target_id ~ '^[A-Za-z0-9._-]+$'),
 check_name text NOT NULL CHECK (check_name IN ('none','failed-jobs','resend-auto','slack-cooldown')),
 PRIMARY KEY(environment_id,channel,target_id)
);
REVOKE ALL ON public.novu_rust_alert_shared_check_bindings FROM PUBLIC;
CREATE FUNCTION public.novu_rust_bind_alert_shared_check(p_env text,p_channel text,p_target text,p_check text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE bound text;
BEGIN
 IF p_env IS NULL OR length(btrim(p_env))=0 OR p_channel IS NULL OR p_channel NOT IN ('slack','email') OR p_target IS NULL OR octet_length(p_target) NOT BETWEEN 1 AND 64 OR p_target !~ '^[A-Za-z0-9._-]+$' OR p_check IS NULL OR p_check NOT IN ('none','failed-jobs','resend-auto','slack-cooldown') THEN
  RAISE EXCEPTION 'invalid shared alert binding' USING ERRCODE='22023';
 END IF;
 IF NOT public.novu_rust_lock_operator_grant(p_env,'alert_send') THEN RAISE EXCEPTION 'alert send permission denied' USING ERRCODE='42501'; END IF;
 IF NOT EXISTS(SELECT 1 FROM public.novu_rust_environments e WHERE e.id=p_env) THEN RAISE EXCEPTION 'environment not found' USING ERRCODE='22023'; END IF;
 INSERT INTO public.novu_rust_alert_shared_check_bindings(environment_id,channel,target_id,check_name) VALUES(p_env,p_channel,p_target,p_check) ON CONFLICT DO NOTHING;
 SELECT b.check_name INTO bound FROM public.novu_rust_alert_shared_check_bindings b WHERE b.environment_id=p_env AND b.channel=p_channel AND b.target_id=p_target FOR SHARE OF b;
 RETURN bound=p_check;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_bind_alert_shared_check(text,text,text,text) FROM PUBLIC;
