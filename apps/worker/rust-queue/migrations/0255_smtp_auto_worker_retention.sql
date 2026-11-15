-- Explicitly delegated retention, separate from heartbeat and alert access.
CREATE TABLE IF NOT EXISTS public.novu_rust_smtp_auto_worker_retention_operators (
  environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),
  database_principal text NOT NULL,
  principal_oid oid NOT NULL,
  PRIMARY KEY (environment_id, database_principal)
);
REVOKE ALL ON public.novu_rust_smtp_auto_worker_retention_operators FROM PUBLIC;

-- Each completed call is recorded in the same transaction as its deletions.
CREATE TABLE IF NOT EXISTS public.novu_rust_smtp_auto_worker_retention_audit (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),
  database_principal text NOT NULL,
  principal_oid oid NOT NULL,
  older_than_seconds integer NOT NULL,
  batch_limit integer NOT NULL,
  deleted_count bigint NOT NULL,
  reason text,
  recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS novu_rust_smtp_auto_worker_retention_audit_env_id_idx
  ON public.novu_rust_smtp_auto_worker_retention_audit(environment_id,id DESC);
REVOKE ALL ON public.novu_rust_smtp_auto_worker_retention_audit FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_prune_smtp_auto_worker_health(
  p_environment text, p_older_than_seconds integer, p_limit integer
) RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_deleted bigint; v_reason text := NULLIF(btrim(pg_catalog.current_setting('swallow.smtp_auto_worker_retention_reason',true)), '');
BEGIN
  -- Maximum health-check age is 86400 seconds. Never remove observations
  -- still eligible for any supported alert threshold.
  IF p_environment IS NULL OR p_environment='' OR p_older_than_seconds IS NULL
     OR p_older_than_seconds NOT BETWEEN 86401 AND 31536000
     OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 1000 THEN
    RAISE EXCEPTION 'invalid SMTP auto worker retention request' USING ERRCODE='22023';
  END IF;
  IF v_reason IS NULL OR octet_length(v_reason)>500 OR v_reason ~ '[[:cntrl:]]' THEN
    RAISE EXCEPTION 'invalid SMTP auto worker retention reason' USING ERRCODE='22023';
  END IF;
  PERFORM 1 FROM public.novu_rust_smtp_auto_worker_retention_operators o
    WHERE o.environment_id=p_environment AND o.database_principal=session_user
      AND o.principal_oid=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin)
    FOR SHARE OF o;
  IF NOT FOUND THEN RAISE EXCEPTION 'SMTP auto worker retention denied' USING ERRCODE='42501'; END IF;
  IF NOT pg_catalog.pg_has_role(current_user,'pg_read_all_stats','MEMBER') THEN
    RAISE EXCEPTION 'SMTP auto worker retention requires database statistics access' USING ERRCODE='42501';
  END IF;
  -- A caller may have inspected statistics earlier in a long transaction.
  PERFORM pg_catalog.pg_stat_clear_snapshot();
  WITH candidates AS (
    SELECT h.environment_id,h.database_principal,h.principal_oid,h.backend_pid,h.backend_started_at
    FROM public.novu_rust_smtp_auto_worker_health h
    WHERE h.environment_id=p_environment
      AND h.observed_at < statement_timestamp()-p_older_than_seconds*interval '1 second'
      AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity a
        WHERE a.pid=h.backend_pid AND a.backend_start=h.backend_started_at AND a.usename=h.database_principal)
    ORDER BY h.observed_at,h.backend_pid,h.backend_started_at
    LIMIT p_limit FOR UPDATE OF h SKIP LOCKED
  )
  DELETE FROM public.novu_rust_smtp_auto_worker_health h USING candidates c
    WHERE h.environment_id=c.environment_id AND h.database_principal=c.database_principal AND h.principal_oid=c.principal_oid AND h.backend_pid=c.backend_pid AND h.backend_started_at=c.backend_started_at;
  GET DIAGNOSTICS v_deleted = ROW_COUNT;
  INSERT INTO public.novu_rust_smtp_auto_worker_retention_audit(environment_id,database_principal,principal_oid,older_than_seconds,batch_limit,deleted_count,reason)
    SELECT p_environment,session_user,oid,p_older_than_seconds,p_limit,v_deleted,v_reason FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin;
  IF NOT FOUND THEN RAISE EXCEPTION 'SMTP auto worker retention principal missing' USING ERRCODE='42501'; END IF;
  RETURN v_deleted;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_prune_smtp_auto_worker_health(text,integer,integer) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_smtp_auto_worker_retention_audit_page(
  p_environment text, p_before_id bigint, p_limit integer
) RETURNS TABLE(id bigint, database_principal text, principal_oid oid,
  older_than_seconds integer, batch_limit integer, deleted_count bigint,
  reason text, recorded_at timestamptz)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  IF p_environment IS NULL OR p_environment='' OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 100
    OR (p_before_id IS NOT NULL AND p_before_id<1) THEN
    RAISE EXCEPTION 'invalid SMTP retention audit request' USING ERRCODE='22023';
  END IF;
  PERFORM 1 FROM public.novu_rust_smtp_auto_worker_retention_operators o
    WHERE o.environment_id=p_environment AND o.database_principal=session_user
      AND o.principal_oid=(SELECT r.oid FROM pg_catalog.pg_roles r WHERE r.rolname=session_user AND r.rolcanlogin)
    FOR SHARE OF o;
  IF NOT FOUND THEN RAISE EXCEPTION 'SMTP auto worker retention audit denied' USING ERRCODE='42501'; END IF;
  RETURN QUERY SELECT a.id,a.database_principal,a.principal_oid,a.older_than_seconds,a.batch_limit,a.deleted_count,a.reason,a.recorded_at
    FROM public.novu_rust_smtp_auto_worker_retention_audit a WHERE a.environment_id=p_environment
      AND (p_before_id IS NULL OR a.id<p_before_id) ORDER BY a.id DESC LIMIT p_limit;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_smtp_auto_worker_retention_audit_page(text,bigint,integer) FROM PUBLIC;
