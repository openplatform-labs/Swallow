-- Optional immutable binding of a target to the Discord cooldown check semantics.
CREATE TABLE public.novu_rust_discord_alert_mode_bindings (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id) ON DELETE CASCADE,
 channel text NOT NULL CHECK (channel IN ('slack','email')),
 target_id text NOT NULL CHECK (octet_length(target_id) BETWEEN 1 AND 64 AND target_id ~ '^[A-Za-z0-9._-]+$'),
 mode text NOT NULL CHECK (mode IN ('none','all','overdue')),
 grace_seconds integer,
 CHECK ((mode='none' AND grace_seconds IS NULL) OR (mode IN ('all','overdue') AND grace_seconds IS NOT NULL AND grace_seconds BETWEEN 1 AND 86400)),
 PRIMARY KEY(environment_id,channel,target_id)
);
REVOKE ALL ON public.novu_rust_discord_alert_mode_bindings FROM PUBLIC;
CREATE FUNCTION public.novu_rust_bind_discord_alert_mode(p_env text,p_channel text,p_target text,p_mode text,p_grace integer)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE bound_mode text; bound_grace integer;
BEGIN
 IF p_env IS NULL OR length(btrim(p_env))=0 OR p_channel IS NULL OR p_channel NOT IN ('slack','email') OR p_target IS NULL OR octet_length(p_target) NOT BETWEEN 1 AND 64 OR p_target !~ '^[A-Za-z0-9._-]+$' OR p_mode IS NULL OR p_mode NOT IN ('none','all','overdue') OR NOT coalesce((p_mode='none' AND p_grace IS NULL) OR (p_mode IN ('all','overdue') AND p_grace BETWEEN 1 AND 86400),false) THEN RAISE EXCEPTION 'invalid Discord alert binding' USING ERRCODE='22023'; END IF;
 IF NOT public.novu_rust_lock_operator_grant(p_env,'alert_send') THEN RAISE EXCEPTION 'alert send permission denied' USING ERRCODE='42501'; END IF;
 IF NOT EXISTS(SELECT 1 FROM public.novu_rust_environments WHERE id=p_env) THEN RAISE EXCEPTION 'environment not found' USING ERRCODE='22023'; END IF;
 INSERT INTO public.novu_rust_discord_alert_mode_bindings(environment_id,channel,target_id,mode,grace_seconds) VALUES(p_env,p_channel,p_target,p_mode,p_grace) ON CONFLICT DO NOTHING;
 SELECT mode,grace_seconds INTO bound_mode,bound_grace FROM public.novu_rust_discord_alert_mode_bindings WHERE environment_id=p_env AND channel=p_channel AND target_id=p_target FOR SHARE;
 RETURN bound_mode=p_mode AND bound_grace IS NOT DISTINCT FROM p_grace;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_bind_discord_alert_mode(text,text,text,text,integer) FROM PUBLIC;
