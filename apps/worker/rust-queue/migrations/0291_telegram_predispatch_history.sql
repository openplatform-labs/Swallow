-- The existing independently granted history viewer can inspect pre-dispatch actions too.
CREATE INDEX novu_rust_telegram_predispatch_retry_history_idx ON public.novu_rust_telegram_predispatch_retries(environment_id,job_id,request_id);
CREATE INDEX novu_rust_telegram_predispatch_cancel_history_idx ON public.novu_rust_telegram_predispatch_cancellations(environment_id,job_id,request_id);
CREATE FUNCTION public.novu_rust_telegram_predispatch_history(p_env text,p_job uuid,p_after uuid,p_after_action text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb;
BEGIN
 IF p_env IS NULL OR length(btrim(p_env))=0 OR p_job IS NULL OR (p_after IS NULL)<>(p_after_action IS NULL) OR (p_after_action IS NOT NULL AND p_after_action NOT IN ('cancel','retry')) THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM 1 FROM public.novu_rust_telegram_history_viewers v JOIN pg_catalog.pg_roles r ON r.rolname=v.database_principal AND r.oid=v.principal_oid AND r.rolcanlogin WHERE v.environment_id=p_env AND v.database_principal=session_user FOR SHARE OF v;
 IF NOT FOUND THEN RAISE EXCEPTION 'history access denied' USING ERRCODE='42501'; END IF;
 WITH entries AS (
  SELECT request_id,job_id,failure_revision,reason,database_principal,principal_oid,created_at,'retry'::text COLLATE "C" AS action FROM public.novu_rust_telegram_predispatch_retries WHERE environment_id=p_env AND job_id=p_job
  UNION ALL
  SELECT request_id,job_id,failure_revision,reason,database_principal,principal_oid,created_at,'cancel'::text COLLATE "C" AS action FROM public.novu_rust_telegram_predispatch_cancellations WHERE environment_id=p_env AND job_id=p_job
 ), candidates AS MATERIALIZED (
  SELECT * FROM entries WHERE p_after IS NULL OR (request_id,action)>(p_after,p_after_action COLLATE "C") ORDER BY request_id,action LIMIT 51
 ), page AS MATERIALIZED (SELECT * FROM candidates ORDER BY request_id,action LIMIT 50)
 SELECT jsonb_build_object('data',COALESCE((SELECT jsonb_agg(jsonb_build_object('requestId',request_id,'jobId',job_id,'failureRevision',failure_revision::text,'action',action,'reason',reason,'databasePrincipal',database_principal,'principalOid',principal_oid::text,'createdAt',created_at) ORDER BY request_id,action) FROM page),'[]'::jsonb),
  'hasMore',(SELECT count(*)>50 FROM candidates),'next',CASE WHEN (SELECT count(*)>50 FROM candidates) THEN (SELECT jsonb_build_object('requestId',request_id,'action',action) FROM page ORDER BY request_id DESC,action DESC LIMIT 1) ELSE NULL END) INTO result;
 RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_telegram_predispatch_history(text,uuid,uuid,text) FROM PUBLIC;
