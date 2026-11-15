-- Explicitly delegated retention, separate from heartbeat and alert access.
CREATE TABLE IF NOT EXISTS public.novu_rust_fcm_health_retention_operators (
  environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),
  database_principal text NOT NULL,
  principal_oid oid NOT NULL,
  PRIMARY KEY (environment_id, database_principal)
);
REVOKE ALL ON public.novu_rust_fcm_health_retention_operators FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_prune_fcm_credential_health(
  p_environment text, p_older_than_seconds integer, p_limit integer
) RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_deleted bigint;
BEGIN
  -- Maximum health-check age is 86400 seconds. Never remove observations
  -- still eligible for any supported alert threshold.
  IF p_environment IS NULL OR p_environment='' OR p_older_than_seconds IS NULL
     OR p_older_than_seconds NOT BETWEEN 86401 AND 31536000
     OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 1000 THEN
    RAISE EXCEPTION 'invalid FCM health retention request' USING ERRCODE='22023';
  END IF;
  PERFORM 1 FROM public.novu_rust_fcm_health_retention_operators o
    WHERE o.environment_id=p_environment AND o.database_principal=session_user
      AND o.principal_oid=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin)
    FOR SHARE OF o;
  IF NOT FOUND THEN RAISE EXCEPTION 'FCM health retention denied' USING ERRCODE='42501'; END IF;
  IF NOT pg_catalog.pg_has_role(current_user,'pg_read_all_stats','MEMBER') THEN
    RAISE EXCEPTION 'FCM health retention requires database statistics access' USING ERRCODE='42501';
  END IF;
  -- A caller may have inspected statistics earlier in a long transaction.
  PERFORM pg_catalog.pg_stat_clear_snapshot();
  WITH candidates AS (
    SELECT h.environment_id,h.instance_id
    FROM public.novu_rust_fcm_credential_health h
    WHERE h.environment_id=p_environment
      AND h.observed_at < statement_timestamp()-p_older_than_seconds*interval '1 second'
      AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity a
        WHERE a.pid=h.backend_pid AND a.backend_start=h.backend_started_at AND a.usename=h.database_principal)
    ORDER BY h.observed_at,h.instance_id
    LIMIT p_limit FOR UPDATE OF h SKIP LOCKED
  )
  DELETE FROM public.novu_rust_fcm_credential_health h USING candidates c
    WHERE h.environment_id=c.environment_id AND h.instance_id=c.instance_id;
  GET DIAGNOSTICS v_deleted = ROW_COUNT;
  RETURN v_deleted;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_prune_fcm_credential_health(text,integer,integer) FROM PUBLIC;
