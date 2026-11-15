-- Explicitly authorized worker logins; no passwords or paths are recorded.
CREATE TABLE public.novu_rust_smtp_ca_workers (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),
 database_principal name NOT NULL, principal_oid oid NOT NULL,
 PRIMARY KEY(environment_id,database_principal)
);
CREATE TABLE public.novu_rust_smtp_ca_health (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id), instance_id uuid NOT NULL,
 database_principal name NOT NULL, backend_pid integer NOT NULL, backend_started_at timestamptz NOT NULL,
 state text NOT NULL CHECK(state IN ('healthy','unhealthy')), observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(environment_id,instance_id)
);
CREATE INDEX novu_rust_smtp_ca_health_env_time_idx ON public.novu_rust_smtp_ca_health(environment_id,observed_at);
REVOKE ALL ON public.novu_rust_smtp_ca_workers,public.novu_rust_smtp_ca_health FROM PUBLIC;
CREATE FUNCTION public.novu_rust_smtp_ca_heartbeat(p_env text,p_instance uuid,p_state text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_env IS NULL OR p_env='' OR p_instance IS NULL OR p_state IS NULL OR p_state NOT IN ('healthy','unhealthy') THEN RAISE EXCEPTION 'invalid SMTP CA health observation' USING ERRCODE='22023'; END IF;
 PERFORM 1 FROM public.novu_rust_smtp_ca_workers w WHERE w.environment_id=p_env AND w.database_principal=session_user AND w.principal_oid=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin) FOR SHARE OF w;
 IF NOT FOUND THEN RAISE EXCEPTION 'SMTP CA heartbeat denied' USING ERRCODE='42501'; END IF;
 IF NOT pg_catalog.pg_has_role(current_user,'pg_read_all_stats','MEMBER') THEN RAISE EXCEPTION 'SMTP CA heartbeat requires database statistics access' USING ERRCODE='42501'; END IF;
 INSERT INTO public.novu_rust_smtp_ca_health(environment_id,instance_id,database_principal,backend_pid,backend_started_at,state)
 SELECT p_env,p_instance,session_user,pg_backend_pid(),a.backend_start,p_state FROM pg_catalog.pg_stat_activity a WHERE a.pid=pg_backend_pid()
 ON CONFLICT(environment_id,instance_id) DO UPDATE SET state=EXCLUDED.state,observed_at=clock_timestamp()
 WHERE public.novu_rust_smtp_ca_health.database_principal=session_user AND public.novu_rust_smtp_ca_health.backend_pid=pg_backend_pid() AND public.novu_rust_smtp_ca_health.backend_started_at=EXCLUDED.backend_started_at;
 RETURN FOUND;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_smtp_ca_heartbeat(text,uuid,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_smtp_ca_health_check(p_env text,p_max_age integer)
RETURNS TABLE(healthy bigint,unhealthy bigint,stale bigint) LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_env IS NULL OR p_env='' OR p_max_age IS NULL OR p_max_age NOT BETWEEN 1 AND 86400 THEN RAISE EXCEPTION 'invalid SMTP CA health request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_assert_alert_view(p_env);
 IF NOT pg_catalog.pg_has_role(current_user,'pg_read_all_stats','MEMBER') THEN RAISE EXCEPTION 'SMTP CA health requires database statistics access' USING ERRCODE='42501'; END IF;
 RETURN QUERY SELECT
 count(*) FILTER(WHERE h.observed_at>=statement_timestamp()-p_max_age*interval '1 second' AND h.state='healthy' AND EXISTS(SELECT 1 FROM pg_catalog.pg_stat_activity a WHERE a.pid=h.backend_pid AND a.backend_start=h.backend_started_at AND a.usename=h.database_principal)),
 count(*) FILTER(WHERE h.observed_at>=statement_timestamp()-p_max_age*interval '1 second' AND h.state='unhealthy'),
 count(*) FILTER(WHERE h.observed_at<statement_timestamp()-p_max_age*interval '1 second')
 FROM public.novu_rust_smtp_ca_health h WHERE h.environment_id=p_env;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_smtp_ca_health_check(text,integer) FROM PUBLIC;
