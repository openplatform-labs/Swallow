-- Fail closed when a scoped login can assume an RLS-bypassing role.
CREATE OR REPLACE FUNCTION public.novu_rust_assert_smtp_scoped_login(p_environment text) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r pg_catalog.pg_roles%ROWTYPE; t text; rel oid;
BEGIN
 SELECT * INTO r FROM pg_catalog.pg_roles WHERE rolname=session_user;
 IF NOT FOUND OR NOT r.rolcanlogin OR r.rolsuper OR r.rolbypassrls OR EXISTS(
  SELECT 1 FROM pg_catalog.pg_roles privileged WHERE (privileged.rolsuper OR privileged.rolbypassrls)
  AND pg_catalog.pg_has_role(r.oid,privileged.oid,'MEMBER')
 ) OR NOT EXISTS(
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
