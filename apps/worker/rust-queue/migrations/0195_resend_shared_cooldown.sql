-- Optional conservative environment-wide Resend cooldown; no automatic activation.
CREATE TABLE public.novu_rust_resend_cooldown_policies (
 environment_id text PRIMARY KEY REFERENCES public.novu_rust_environments(id),
 fallback_seconds integer NOT NULL CHECK(fallback_seconds BETWEEN 1 AND 86400),
 not_before timestamptz NOT NULL DEFAULT '-infinity'
);
CREATE TABLE public.novu_rust_resend_cooldown_workers (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),
 database_principal name NOT NULL, principal_oid oid NOT NULL,
 PRIMARY KEY(environment_id,database_principal)
);
REVOKE ALL ON public.novu_rust_resend_cooldown_policies,public.novu_rust_resend_cooldown_workers FROM PUBLIC;
CREATE FUNCTION public.novu_rust_resend_cooldown_access(p_env text) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 PERFORM 1 FROM public.novu_rust_resend_cooldown_workers w JOIN pg_catalog.pg_roles r ON r.rolname=w.database_principal AND r.oid=w.principal_oid WHERE w.environment_id=p_env AND w.database_principal=session_user FOR SHARE OF w;
 IF NOT FOUND THEN RAISE EXCEPTION 'Resend cooldown access denied' USING ERRCODE='42501'; END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_cooldown_access(text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_resend_cooldown_check(p_env text) RETURNS text
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_until timestamptz;
BEGIN
 PERFORM public.novu_rust_resend_cooldown_access(p_env);
 SELECT not_before INTO v_until FROM public.novu_rust_resend_cooldown_policies WHERE environment_id=p_env FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Resend cooldown policy missing' USING ERRCODE='42501'; END IF;
 RETURN CASE WHEN v_until>clock_timestamp() THEN v_until::text ELSE NULL END;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_cooldown_check(text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_resend_cooldown_record(p_env text,p_seconds integer) RETURNS text
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_until timestamptz;
BEGIN
 IF p_seconds IS NOT NULL AND p_seconds NOT BETWEEN 0 AND 86400 THEN RAISE EXCEPTION 'invalid Resend cooldown' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_resend_cooldown_access(p_env);
 UPDATE public.novu_rust_resend_cooldown_policies SET not_before=greatest(not_before,clock_timestamp()+make_interval(secs=>coalesce(p_seconds,fallback_seconds)::double precision)) WHERE environment_id=p_env RETURNING not_before INTO v_until;
 IF NOT FOUND THEN RAISE EXCEPTION 'Resend cooldown policy missing' USING ERRCODE='42501'; END IF;
 RETURN v_until::text;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_cooldown_record(text,integer) FROM PUBLIC;

-- New workers bind the optional guard alongside their existing idempotency policy.
ALTER TABLE public.novu_rust_resend_idempotency_policy ADD COLUMN shared_cooldown boolean;
CREATE FUNCTION public.novu_rust_check_resend_shared_cooldown(p_environment text,p_enabled boolean)
RETURNS void LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public AS $$
DECLARE v_enabled boolean;
BEGIN
 SELECT shared_cooldown INTO v_enabled FROM public.novu_rust_resend_idempotency_policy WHERE environment_id=p_environment FOR SHARE;
 IF p_enabled IS NULL OR v_enabled IS DISTINCT FROM p_enabled THEN
  RAISE EXCEPTION 'Resend shared cooldown policy mismatch' USING ERRCODE='55000';
 END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_check_resend_shared_cooldown(text,boolean) FROM PUBLIC;
