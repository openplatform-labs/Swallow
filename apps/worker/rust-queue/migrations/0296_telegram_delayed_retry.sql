ALTER TABLE public.novu_rust_telegram_resolutions DROP CONSTRAINT novu_rust_telegram_resolutions_decision_check;
ALTER TABLE public.novu_rust_telegram_resolutions ADD CONSTRAINT novu_rust_telegram_resolutions_decision_check CHECK (decision IN ('retry','accepted','retry_after'));
ALTER TABLE public.novu_rust_telegram_resolutions ADD COLUMN retry_after_seconds integer CHECK (retry_after_seconds BETWEEN 0 AND 86400), ADD COLUMN retry_not_before timestamptz;

-- Reuse the existing request namespace and retry permission. The advisory lock
-- serializes this operation with immediate retries using the same request ID.
CREATE FUNCTION public.novu_rust_telegram_resolve_after(p_env text,p_request uuid,p_job uuid,p_attempt uuid,p_reason text) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_oid oid; r public.novu_rust_telegram_resolutions%ROWTYPE; a public.novu_rust_telegram_attempts%ROWTYPE; v_status text; v_until timestamptz;
BEGIN
 IF p_request IS NULL OR p_job IS NULL OR p_attempt IS NULL OR p_reason IS NULL OR octet_length(btrim(p_reason)) NOT BETWEEN 1 AND 512 THEN RAISE EXCEPTION 'invalid Telegram decision' USING ERRCODE='22023'; END IF;
 v_oid:=public.novu_rust_telegram_operator_access(p_env,'retry');
 PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(pg_catalog.jsonb_build_array('telegram-decision',p_env,p_request)::text,0));
 SELECT * INTO r FROM public.novu_rust_telegram_resolutions WHERE environment_id=p_env AND request_id=p_request;
 IF FOUND THEN
  IF (r.job_id,r.attempt_id,r.decision,r.reason,r.database_principal,r.principal_oid) IS DISTINCT FROM (p_job,p_attempt,'retry_after',p_reason,session_user,v_oid) THEN RAISE EXCEPTION 'Telegram decision conflict' USING ERRCODE='P4090'; END IF;
  RETURN jsonb_build_object('recorded',true,'replayed',true,'requestId',p_request,'retryNotBefore',r.retry_not_before);
 END IF;
 SELECT j.status INTO v_status FROM public.novu_queue_jobs j WHERE j.environment_id=p_env AND j.id=p_job AND j.kind IN ('experimental_telegram','experimental_telegram_workflow') FOR UPDATE OF j;
 IF NOT FOUND THEN RAISE EXCEPTION 'stale Telegram job' USING ERRCODE='22023'; END IF;
 SELECT * INTO a FROM public.novu_rust_telegram_attempts WHERE environment_id=p_env AND job_id=p_job FOR UPDATE;
 IF NOT FOUND OR v_status<>'failed' OR a.attempt_id IS DISTINCT FROM p_attempt OR a.state<>'rejected' OR a.http_status IS DISTINCT FROM 429 OR a.retry_after_seconds IS NULL THEN RAISE EXCEPTION 'current Telegram 429 with retry guidance required' USING ERRCODE='22023'; END IF;
 v_until:=greatest(clock_timestamp(),a.updated_at + a.retry_after_seconds * interval '1 second');
 INSERT INTO public.novu_rust_telegram_resolutions(environment_id,request_id,job_id,attempt_id,previous_state,previous_http_status,previous_updated_at,decision,reason,database_principal,principal_oid,retry_after_seconds,retry_not_before)
 VALUES(p_env,p_request,p_job,p_attempt,a.state,a.http_status,a.updated_at,'retry_after',p_reason,session_user,v_oid,a.retry_after_seconds,v_until);
 DELETE FROM public.novu_rust_telegram_attempts WHERE environment_id=p_env AND job_id=p_job AND attempt_id=p_attempt;
 UPDATE public.novu_queue_jobs SET status='pending',available_at=v_until,last_error=NULL WHERE environment_id=p_env AND id=p_job;
 RETURN jsonb_build_object('recorded',true,'replayed',false,'requestId',p_request,'retryNotBefore',v_until);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_telegram_resolve_after(text,uuid,uuid,uuid,text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_telegram_resolution_history(p_env text,p_job uuid,p_after uuid) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb;
BEGIN
 IF p_env IS NULL OR length(btrim(p_env))=0 OR p_job IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM 1 FROM public.novu_rust_telegram_history_viewers v JOIN pg_catalog.pg_roles r ON r.rolname=v.database_principal AND r.oid=v.principal_oid AND r.rolcanlogin WHERE v.environment_id=p_env AND v.database_principal=session_user FOR SHARE OF v;
 IF NOT FOUND THEN RAISE EXCEPTION 'history access denied' USING ERRCODE='42501'; END IF;
 WITH candidates AS MATERIALIZED (SELECT * FROM public.novu_rust_telegram_resolutions r WHERE r.environment_id=p_env AND r.job_id=p_job AND (p_after IS NULL OR r.request_id>p_after) ORDER BY r.request_id LIMIT 51),page AS MATERIALIZED (SELECT * FROM candidates ORDER BY request_id LIMIT 50)
 SELECT jsonb_build_object('data',COALESCE((SELECT jsonb_agg(jsonb_build_object('requestId',request_id,'jobId',job_id,'attemptId',attempt_id,'previousState',previous_state,'httpStatus',previous_http_status,'previousUpdatedAt',previous_updated_at,'decision',decision,'retryAfterSeconds',retry_after_seconds,'retryNotBefore',retry_not_before,'reason',reason,'databasePrincipal',database_principal,'principalOid',principal_oid::text,'createdAt',created_at) ORDER BY request_id) FROM page),'[]'::jsonb),'hasMore',(SELECT count(*)>50 FROM candidates),'next',CASE WHEN (SELECT count(*)>50 FROM candidates) THEN (SELECT request_id FROM page ORDER BY request_id DESC LIMIT 1) ELSE NULL END) INTO result;
 RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_telegram_resolution_history(text,uuid,uuid) FROM PUBLIC;
