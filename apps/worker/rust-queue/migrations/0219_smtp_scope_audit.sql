-- Audit direct scoped-login registration and state changes; no historical
-- attribution can be inferred for rows created before this migration.
CREATE TABLE public.novu_rust_smtp_scope_audit (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 principal_oid oid NOT NULL, database_principal name NOT NULL,
 previous_environment text, environment_id text NOT NULL,
 previous_enabled boolean, enabled boolean NOT NULL,
 actor name NOT NULL DEFAULT session_user, actor_oid oid,
 recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX novu_rust_smtp_scope_audit_env_idx ON public.novu_rust_smtp_scope_audit(environment_id,id);
CREATE INDEX novu_rust_smtp_scope_audit_previous_env_idx ON public.novu_rust_smtp_scope_audit(previous_environment,id) WHERE previous_environment IS NOT NULL;
REVOKE ALL ON public.novu_rust_smtp_scope_audit FROM PUBLIC;
CREATE FUNCTION public.novu_rust_smtp_scope_audit_trigger() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF TG_OP='INSERT' OR (OLD.environment_id,OLD.enabled) IS DISTINCT FROM (NEW.environment_id,NEW.enabled) THEN
  INSERT INTO public.novu_rust_smtp_scope_audit(principal_oid,database_principal,previous_environment,environment_id,previous_enabled,enabled,actor_oid)
  VALUES(NEW.principal_oid,NEW.database_principal,CASE WHEN TG_OP='INSERT' THEN NULL ELSE OLD.environment_id END,NEW.environment_id,CASE WHEN TG_OP='INSERT' THEN NULL ELSE OLD.enabled END,NEW.enabled,(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=session_user));
 END IF;
 RETURN NEW;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_smtp_scope_audit_trigger() FROM PUBLIC;
CREATE TRIGGER novu_rust_smtp_scope_audit_trigger AFTER INSERT OR UPDATE ON public.novu_rust_smtp_scoped_logins
 FOR EACH ROW EXECUTE FUNCTION public.novu_rust_smtp_scope_audit_trigger();

CREATE TABLE public.novu_rust_smtp_scope_audit_viewers (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),
 database_principal name NOT NULL,principal_oid oid NOT NULL,
 PRIMARY KEY(environment_id,database_principal)
);
REVOKE ALL ON public.novu_rust_smtp_scope_audit_viewers FROM PUBLIC;
CREATE FUNCTION public.novu_rust_smtp_scope_audit_read(p_environment text,p_after bigint)
RETURNS TABLE(id text,database_principal text,principal_oid text,previous_environment text,environment_id text,previous_enabled boolean,enabled boolean,actor text,actor_oid text,recorded_at text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_environment IS NULL OR length(btrim(p_environment))=0 OR p_after IS NULL OR p_after<0 THEN RAISE EXCEPTION 'invalid scope audit request' USING ERRCODE='22023'; END IF;
 PERFORM 1 FROM public.novu_rust_smtp_scope_audit_viewers v JOIN pg_catalog.pg_roles r ON r.oid=v.principal_oid AND r.rolname=v.database_principal
 WHERE v.environment_id=p_environment AND v.database_principal=session_user AND r.rolcanlogin FOR SHARE OF v;
 IF NOT FOUND THEN RAISE EXCEPTION 'SMTP scope audit access denied' USING ERRCODE='42501'; END IF;
 RETURN QUERY SELECT a.id::text,a.database_principal::text,a.principal_oid::text,CASE WHEN a.previous_environment=p_environment THEN a.previous_environment ELSE NULL END,CASE WHEN a.environment_id=p_environment THEN a.environment_id ELSE NULL END,a.previous_enabled,a.enabled,a.actor::text,a.actor_oid::text,a.recorded_at::text
 FROM public.novu_rust_smtp_scope_audit a WHERE (a.environment_id=p_environment OR a.previous_environment=p_environment) AND a.id>p_after ORDER BY a.id LIMIT 100;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_smtp_scope_audit_read(text,bigint) FROM PUBLIC;
