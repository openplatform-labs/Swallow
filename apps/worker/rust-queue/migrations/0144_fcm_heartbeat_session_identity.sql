-- A PostgreSQL PID can be reused after a connection ends. Do not let a new
-- session reuse an old observation even if it supplies the same instance ID.
CREATE OR REPLACE FUNCTION public.novu_rust_fcm_credential_heartbeat(p_environment text,p_instance uuid,p_state text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  IF p_environment IS NULL OR p_environment='' OR p_instance IS NULL OR p_state IS NULL OR p_state NOT IN ('healthy','unhealthy') THEN
    RAISE EXCEPTION 'invalid credential health observation' USING ERRCODE='22023';
  END IF;
  PERFORM 1 FROM public.novu_rust_fcm_routing_workers w
    WHERE w.environment_id=p_environment AND w.database_principal=session_user
      AND w.principal_oid=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin)
    FOR SHARE OF w;
  IF NOT FOUND THEN RAISE EXCEPTION 'FCM credential heartbeat denied' USING ERRCODE='42501'; END IF;
  IF NOT pg_catalog.pg_has_role(current_user,'pg_read_all_stats','MEMBER') THEN
    RAISE EXCEPTION 'FCM credential heartbeat requires database statistics access' USING ERRCODE='42501';
  END IF;
  INSERT INTO public.novu_rust_fcm_credential_health(environment_id,instance_id,database_principal,backend_pid,backend_started_at,state)
    SELECT p_environment,p_instance,session_user,pg_backend_pid(),a.backend_start,p_state FROM pg_catalog.pg_stat_activity a WHERE a.pid=pg_backend_pid()
    ON CONFLICT(environment_id,instance_id) DO UPDATE SET state=EXCLUDED.state,observed_at=clock_timestamp()
    WHERE public.novu_rust_fcm_credential_health.database_principal=EXCLUDED.database_principal
      AND public.novu_rust_fcm_credential_health.backend_pid=EXCLUDED.backend_pid
      AND public.novu_rust_fcm_credential_health.backend_started_at=EXCLUDED.backend_started_at;
  RETURN FOUND;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_fcm_credential_heartbeat(text,uuid,text) FROM PUBLIC;
