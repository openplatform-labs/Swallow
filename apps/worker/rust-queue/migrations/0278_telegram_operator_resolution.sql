-- Stable identity for each current attempt, including rows created by old workers.
ALTER TABLE public.novu_rust_telegram_attempts ADD COLUMN attempt_id uuid NOT NULL DEFAULT gen_random_uuid();
CREATE TABLE public.novu_rust_telegram_operator_grants (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id), database_principal name NOT NULL, principal_oid oid NOT NULL,
 can_view boolean NOT NULL DEFAULT false, can_retry boolean NOT NULL DEFAULT false, can_accept boolean NOT NULL DEFAULT false,
 PRIMARY KEY(environment_id,database_principal)
);
CREATE TABLE public.novu_rust_telegram_resolutions (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id), request_id uuid NOT NULL, job_id uuid NOT NULL,
 attempt_id uuid NOT NULL, previous_state text NOT NULL CHECK(previous_state IN ('unknown','rejected')),
 previous_http_status integer, previous_updated_at timestamptz NOT NULL, decision text NOT NULL CHECK(decision IN ('retry','accepted')),
 reason text NOT NULL, database_principal name NOT NULL, principal_oid oid NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(environment_id,request_id)
);
REVOKE ALL ON public.novu_rust_telegram_operator_grants,public.novu_rust_telegram_resolutions FROM PUBLIC;
CREATE FUNCTION public.novu_rust_telegram_operator_access(p_env text,p_action text) RETURNS oid LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_oid oid;
BEGIN
 IF p_env IS NULL OR length(btrim(p_env))=0 OR p_action IS NULL OR p_action NOT IN ('view','retry','accepted') THEN RAISE EXCEPTION 'invalid Telegram access request' USING ERRCODE='22023'; END IF;
 SELECT g.principal_oid INTO v_oid FROM public.novu_rust_telegram_operator_grants g JOIN pg_catalog.pg_roles r ON r.oid=g.principal_oid AND r.rolname=g.database_principal AND r.rolcanlogin
 WHERE g.environment_id=p_env AND g.database_principal=session_user AND CASE p_action WHEN 'view' THEN g.can_view WHEN 'retry' THEN g.can_retry ELSE g.can_accept END FOR SHARE OF g;
 IF NOT FOUND THEN RAISE EXCEPTION 'Telegram operator access denied' USING ERRCODE='42501'; END IF;
 RETURN v_oid;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_telegram_operator_access(text,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_telegram_attention(p_env text,p_after uuid) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb;
BEGIN
 PERFORM public.novu_rust_telegram_operator_access(p_env,'view');
 WITH candidates AS MATERIALIZED (
  SELECT j.id,j.status,j.attempts,a.attempt_id,a.state,a.http_status,a.updated_at FROM public.novu_queue_jobs j
  JOIN public.novu_rust_telegram_attempts a ON a.job_id=j.id AND a.environment_id=j.environment_id
  WHERE j.environment_id=p_env AND j.kind='experimental_telegram' AND j.status='failed' AND a.state IN ('unknown','rejected') AND (p_after IS NULL OR j.id>p_after)
  ORDER BY j.id LIMIT 51
 ), page AS MATERIALIZED (SELECT * FROM candidates ORDER BY id LIMIT 50)
 SELECT jsonb_build_object('data',COALESCE((SELECT jsonb_agg(jsonb_build_object('jobId',id,'jobStatus',status,'attempts',attempts,'attemptId',attempt_id,'state',state,'httpStatus',http_status,'updatedAt',updated_at) ORDER BY id) FROM page),'[]'::jsonb),'hasMore',(SELECT count(*)>50 FROM candidates),'next',CASE WHEN (SELECT count(*)>50 FROM candidates) THEN (SELECT id FROM page ORDER BY id DESC LIMIT 1) ELSE NULL END) INTO result;
 RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_telegram_attention(text,uuid) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_telegram_resolve(p_env text,p_request uuid,p_job uuid,p_attempt uuid,p_decision text,p_reason text) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_oid oid; r public.novu_rust_telegram_resolutions%ROWTYPE; v_state text; v_code integer; v_attempt uuid; v_status text; v_updated timestamptz;
BEGIN
 IF p_request IS NULL OR p_job IS NULL OR p_attempt IS NULL OR p_decision IS NULL OR p_decision NOT IN ('retry','accepted') OR p_reason IS NULL OR octet_length(btrim(p_reason)) NOT BETWEEN 1 AND 512 THEN RAISE EXCEPTION 'invalid Telegram decision' USING ERRCODE='22023'; END IF;
 v_oid:=public.novu_rust_telegram_operator_access(p_env,p_decision);
 PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(pg_catalog.jsonb_build_array('telegram-decision',p_env,p_request)::text,0));
 SELECT * INTO r FROM public.novu_rust_telegram_resolutions WHERE environment_id=p_env AND request_id=p_request;
 IF FOUND THEN
  IF (r.job_id,r.attempt_id,r.decision,r.reason,r.database_principal,r.principal_oid) IS DISTINCT FROM (p_job,p_attempt,p_decision,p_reason,session_user,v_oid) THEN RAISE EXCEPTION 'Telegram decision conflict' USING ERRCODE='P4090'; END IF;
  RETURN jsonb_build_object('recorded',true,'replayed',true,'requestId',p_request);
 END IF;
 SELECT j.status,a.state,a.http_status,a.attempt_id,a.updated_at INTO v_status,v_state,v_code,v_attempt,v_updated FROM public.novu_queue_jobs j
 JOIN public.novu_rust_telegram_attempts a ON a.job_id=j.id AND a.environment_id=j.environment_id
 WHERE j.environment_id=p_env AND j.id=p_job AND j.kind='experimental_telegram' FOR UPDATE OF j,a;
 IF NOT FOUND OR v_attempt IS DISTINCT FROM p_attempt OR v_status<>'failed' OR v_state NOT IN ('unknown','rejected') OR (p_decision='accepted' AND v_state<>'unknown') THEN RAISE EXCEPTION 'stale or ineligible Telegram attempt' USING ERRCODE='22023'; END IF;
 INSERT INTO public.novu_rust_telegram_resolutions(environment_id,request_id,job_id,attempt_id,previous_state,previous_http_status,previous_updated_at,decision,reason,database_principal,principal_oid) VALUES(p_env,p_request,p_job,p_attempt,v_state,v_code,v_updated,p_decision,p_reason,session_user,v_oid);
 IF p_decision='retry' THEN
  DELETE FROM public.novu_rust_telegram_attempts WHERE job_id=p_job AND environment_id=p_env AND attempt_id=p_attempt;
  UPDATE public.novu_queue_jobs SET status='pending',available_at=clock_timestamp(),last_error=NULL WHERE environment_id=p_env AND id=p_job;
 ELSE
  UPDATE public.novu_rust_telegram_attempts SET state='accepted',updated_at=clock_timestamp() WHERE job_id=p_job AND environment_id=p_env AND attempt_id=p_attempt;
  UPDATE public.novu_queue_jobs SET status='completed',last_error=NULL WHERE environment_id=p_env AND id=p_job;
 END IF;
 RETURN jsonb_build_object('recorded',true,'replayed',false,'requestId',p_request);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_telegram_resolve(text,uuid,uuid,uuid,text,text) FROM PUBLIC;
