-- A separate log gives retries stable idempotency and an atomic audit record.
CREATE TABLE public.novu_rust_telegram_predispatch_retries (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),
 request_id uuid NOT NULL, job_id uuid NOT NULL, failure_revision bigint NOT NULL,
 reason text NOT NULL, database_principal name NOT NULL, principal_oid oid NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY(environment_id,request_id)
);
REVOKE ALL ON public.novu_rust_telegram_predispatch_retries FROM PUBLIC;

CREATE FUNCTION public.novu_rust_telegram_predispatch_retry(p_env text,p_request uuid,p_job uuid,p_revision bigint,p_reason text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_oid oid; r public.novu_rust_telegram_predispatch_retries%ROWTYPE; v_revision bigint;
BEGIN
 IF p_request IS NULL OR p_job IS NULL OR p_revision IS NULL OR p_revision<0 OR p_reason IS NULL OR octet_length(btrim(p_reason)) NOT BETWEEN 1 AND 512 THEN
  RAISE EXCEPTION 'invalid Telegram pre-dispatch retry' USING ERRCODE='22023';
 END IF;
 v_oid:=public.novu_rust_telegram_operator_access(p_env,'retry');
 PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(pg_catalog.jsonb_build_array('telegram-predispatch-retry',p_env,p_request)::text,0));
 SELECT * INTO r FROM public.novu_rust_telegram_predispatch_retries WHERE environment_id=p_env AND request_id=p_request;
 IF FOUND THEN
  IF (r.job_id,r.failure_revision,r.reason,r.database_principal,r.principal_oid) IS DISTINCT FROM (p_job,p_revision,p_reason,session_user,v_oid) THEN
   RAISE EXCEPTION 'Telegram pre-dispatch retry conflict' USING ERRCODE='P4090';
  END IF;
  RETURN jsonb_build_object('recorded',true,'replayed',true,'requestId',p_request);
 END IF;
 SELECT j.failure_revision INTO v_revision FROM public.novu_queue_jobs j
  WHERE j.environment_id=p_env AND j.id=p_job AND j.kind IN ('experimental_telegram','experimental_telegram_workflow') AND j.status='failed' FOR UPDATE OF j;
 IF NOT FOUND OR v_revision<>p_revision OR EXISTS(SELECT 1 FROM public.novu_rust_telegram_attempts a WHERE a.job_id=p_job AND a.environment_id=p_env) THEN
  RAISE EXCEPTION 'stale or ineligible Telegram failure' USING ERRCODE='22023';
 END IF;
 INSERT INTO public.novu_rust_telegram_predispatch_retries(environment_id,request_id,job_id,failure_revision,reason,database_principal,principal_oid)
  VALUES(p_env,p_request,p_job,p_revision,p_reason,session_user,v_oid);
 UPDATE public.novu_queue_jobs SET status='pending',available_at=clock_timestamp(),last_error=NULL,lease_token=NULL,lease_until=NULL WHERE environment_id=p_env AND id=p_job;
 RETURN jsonb_build_object('recorded',true,'replayed',false,'requestId',p_request);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_telegram_predispatch_retry(text,uuid,uuid,bigint,text) FROM PUBLIC;
