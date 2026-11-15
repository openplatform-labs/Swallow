-- Optional SMTP worker row isolation. Installation is explicit because it changes shared tables.
CREATE TABLE public.novu_rust_smtp_scoped_logins (
 principal_oid oid PRIMARY KEY,
 database_principal name NOT NULL,
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),
 enabled boolean NOT NULL DEFAULT true
);
REVOKE ALL ON public.novu_rust_smtp_scoped_logins FROM PUBLIC;

-- Keep registrations sticky: removing or changing identity could restore the
-- unrestricted path. Normal revocation sets enabled=false.
CREATE FUNCTION public.novu_rust_smtp_scoped_login_sticky() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF TG_OP='DELETE' OR NEW.principal_oid IS DISTINCT FROM OLD.principal_oid OR NEW.database_principal IS DISTINCT FROM OLD.database_principal THEN
  RAISE EXCEPTION 'SMTP scoped login identity is immutable; disable it instead' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_smtp_scoped_login_sticky() FROM PUBLIC;
CREATE TRIGGER novu_rust_smtp_scoped_login_sticky BEFORE UPDATE OR DELETE ON public.novu_rust_smtp_scoped_logins FOR EACH ROW EXECUTE FUNCTION public.novu_rust_smtp_scoped_login_sticky();

-- Unregistered logins retain existing behavior. A registered login is restricted
-- by session_user even if it changes current role; disable rather than delete
-- a registration to revoke its access without restoring the unrestricted path.
CREATE FUNCTION public.novu_rust_smtp_rls_allows(p_environment text) RETURNS boolean
LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
 SELECT COALESCE((SELECT COALESCE(g.enabled AND g.database_principal=session_user AND g.environment_id=p_environment,false)
   FROM public.novu_rust_smtp_scoped_logins g
   WHERE g.principal_oid=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=session_user)),true)
$$;
GRANT EXECUTE ON FUNCTION public.novu_rust_smtp_rls_allows(text) TO PUBLIC;

-- Explicit, transactional install. Refuse pre-existing RLS/policies instead of
-- replacing a deployment's isolation configuration. The caller must own tables.
CREATE FUNCTION public.novu_rust_install_smtp_scoped_rls() RETURNS void
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE t text; rel oid;
BEGIN
 FOREACH t IN ARRAY ARRAY['novu_queue_jobs','novu_rust_smtp_attempts','novu_rust_email_workflow_worker_config','novu_rust_subscribers','novu_rust_workflow_preferences','novu_rust_subscriber_preferences','novu_rust_subscriber_workflow_preferences','novu_rust_workflows','novu_rust_contexts','novu_rust_email_messages','novu_rust_email_step_outcomes'] LOOP
  SELECT c.oid INTO rel FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname=t AND c.relkind IN ('r','p');
  IF rel IS NULL OR EXISTS(SELECT 1 FROM pg_catalog.pg_class c WHERE c.oid=rel AND (c.relrowsecurity OR c.relforcerowsecurity)) OR EXISTS(SELECT 1 FROM pg_catalog.pg_policy WHERE polrelid=rel) THEN
   RAISE EXCEPTION 'cannot install SMTP RLS on existing protected or missing table %',t USING ERRCODE='55000';
  END IF;
  EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY',t);
  EXECUTE format('CREATE POLICY swallow_smtp_baseline ON public.%I AS PERMISSIVE FOR ALL TO PUBLIC USING (true) WITH CHECK (true)',t);
  EXECUTE format('CREATE POLICY swallow_smtp_restrict ON public.%I AS RESTRICTIVE FOR ALL TO PUBLIC USING (public.novu_rust_smtp_rls_allows(environment_id)) WITH CHECK (public.novu_rust_smtp_rls_allows(environment_id))',t);
 END LOOP;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_install_smtp_scoped_rls() FROM PUBLIC;

-- Startup guard for workers opting into scoped RLS. Fail closed if the login,
-- registration or table protection is not suitable. DBA-level changes remain trusted.
CREATE FUNCTION public.novu_rust_assert_smtp_scoped_login(p_environment text) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r pg_catalog.pg_roles%ROWTYPE; t text; rel oid;
BEGIN
 SELECT * INTO r FROM pg_catalog.pg_roles WHERE rolname=session_user;
 IF NOT FOUND OR NOT r.rolcanlogin OR r.rolsuper OR r.rolbypassrls OR NOT EXISTS(
  SELECT 1 FROM public.novu_rust_smtp_scoped_logins g WHERE g.principal_oid=r.oid AND g.database_principal=session_user AND g.enabled AND g.environment_id=p_environment
 ) THEN RAISE EXCEPTION 'SMTP scoped login unavailable' USING ERRCODE='42501'; END IF;
 FOREACH t IN ARRAY ARRAY['novu_queue_jobs','novu_rust_smtp_attempts','novu_rust_email_workflow_worker_config','novu_rust_subscribers','novu_rust_workflow_preferences','novu_rust_subscriber_preferences','novu_rust_subscriber_workflow_preferences','novu_rust_workflows','novu_rust_contexts','novu_rust_email_messages','novu_rust_email_step_outcomes'] LOOP
  SELECT c.oid INTO rel FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname=t AND c.relrowsecurity AND NOT pg_catalog.pg_has_role(r.oid,c.relowner,'MEMBER');
  IF rel IS NULL OR NOT EXISTS(SELECT 1 FROM pg_catalog.pg_policy p WHERE p.polrelid=rel AND p.polname='swallow_smtp_baseline' AND p.polpermissive) OR NOT EXISTS(SELECT 1 FROM pg_catalog.pg_policy p WHERE p.polrelid=rel AND p.polname='swallow_smtp_restrict' AND NOT p.polpermissive) THEN
   RAISE EXCEPTION 'SMTP scoped isolation unavailable' USING ERRCODE='42501';
  END IF;
 END LOOP;
END; $$;
GRANT EXECUTE ON FUNCTION public.novu_rust_assert_smtp_scoped_login(text) TO PUBLIC;
