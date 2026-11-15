-- Optional observation commits atomically with a successful retry batch.
CREATE TABLE public.novu_rust_smtp_auto_worker_health (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),
 database_principal name NOT NULL,principal_oid oid NOT NULL,backend_pid integer NOT NULL,
 backend_started_at timestamptz NOT NULL,observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(environment_id,database_principal,principal_oid,backend_pid,backend_started_at)
);
CREATE INDEX novu_rust_smtp_auto_worker_health_env_time ON public.novu_rust_smtp_auto_worker_health(environment_id,observed_at);
REVOKE ALL ON public.novu_rust_smtp_auto_worker_health FROM PUBLIC;
CREATE FUNCTION public.novu_rust_smtp_auto_retry_batch_observed(p_env text,p_limit integer)
RETURNS integer LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE n integer; v_oid oid;
BEGIN
 IF NOT pg_catalog.pg_has_role(current_user,'pg_read_all_stats','MEMBER') THEN RAISE EXCEPTION 'SMTP auto worker health requires statistics access' USING ERRCODE='42501'; END IF;
 n:=public.novu_rust_smtp_auto_retry_batch(p_env,p_limit);
 SELECT oid INTO v_oid FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin;
 INSERT INTO public.novu_rust_smtp_auto_worker_health(environment_id,database_principal,principal_oid,backend_pid,backend_started_at)
 SELECT p_env,session_user,v_oid,pg_backend_pid(),a.backend_start FROM pg_catalog.pg_stat_activity a WHERE a.pid=pg_backend_pid()
 ON CONFLICT(environment_id,database_principal,principal_oid,backend_pid,backend_started_at) DO UPDATE SET observed_at=clock_timestamp();
 IF NOT FOUND THEN RAISE EXCEPTION 'SMTP auto worker session missing' USING ERRCODE='55000'; END IF;
 RETURN n;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_smtp_auto_retry_batch_observed(text,integer) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_smtp_auto_worker_health_check(p_env text,p_max_age integer)
RETURNS TABLE(healthy bigint,stale bigint) LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_env IS NULL OR p_env='' OR p_max_age IS NULL OR p_max_age NOT BETWEEN 1 AND 86400 THEN RAISE EXCEPTION 'invalid SMTP auto worker health request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_assert_alert_view(p_env);
 IF NOT pg_catalog.pg_has_role(current_user,'pg_read_all_stats','MEMBER') THEN RAISE EXCEPTION 'SMTP auto worker health requires statistics access' USING ERRCODE='42501'; END IF;
 RETURN QUERY WITH observations AS (
  SELECT h.observed_at>=statement_timestamp()-p_max_age*interval '1 second' AND EXISTS(
   SELECT 1 FROM public.novu_rust_smtp_auto_retry_workers w WHERE w.environment_id=h.environment_id AND w.database_principal=h.database_principal AND w.principal_oid=h.principal_oid
  ) AND EXISTS(
   SELECT 1 FROM pg_catalog.pg_stat_activity a JOIN pg_catalog.pg_roles r ON r.rolname=a.usename
   WHERE a.pid=h.backend_pid AND a.backend_start=h.backend_started_at AND a.usename=h.database_principal AND r.oid=h.principal_oid
  ) AS live FROM public.novu_rust_smtp_auto_worker_health h WHERE h.environment_id=p_env
 ) SELECT count(*) FILTER(WHERE live),count(*) FILTER(WHERE NOT live) FROM observations;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_smtp_auto_worker_health_check(text,integer) FROM PUBLIC;
