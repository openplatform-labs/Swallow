-- Cancellation is a separately granted operator decision; it does not establish delivery outcome.
CREATE TABLE public.novu_rust_telegram_predispatch_cancel_grants (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id) ON DELETE CASCADE,
 database_principal name NOT NULL, principal_oid oid NOT NULL,
 PRIMARY KEY(environment_id,database_principal)
);
CREATE TABLE public.novu_rust_telegram_predispatch_cancellations (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id), request_id uuid NOT NULL,
 job_id uuid NOT NULL, failure_revision bigint NOT NULL, reason text NOT NULL,
 database_principal name NOT NULL, principal_oid oid NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY(environment_id,request_id)
);
REVOKE ALL ON public.novu_rust_telegram_predispatch_cancel_grants,public.novu_rust_telegram_predispatch_cancellations FROM PUBLIC;
CREATE FUNCTION public.novu_rust_telegram_predispatch_cancel(p_env text,p_request uuid,p_job uuid,p_revision bigint,p_reason text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_oid oid; v_revision bigint; r public.novu_rust_telegram_predispatch_cancellations%ROWTYPE;
BEGIN
 IF p_env IS NULL OR length(btrim(p_env))=0 OR p_request IS NULL OR p_job IS NULL OR p_revision IS NULL OR p_revision<0 OR p_reason IS NULL OR octet_length(btrim(p_reason)) NOT BETWEEN 1 AND 512 THEN
  RAISE EXCEPTION 'invalid Telegram cancellation' USING ERRCODE='22023';
 END IF;
 SELECT g.principal_oid INTO v_oid FROM public.novu_rust_telegram_predispatch_cancel_grants g JOIN pg_catalog.pg_roles role ON role.oid=g.principal_oid AND role.rolname=g.database_principal AND role.rolcanlogin
 WHERE g.environment_id=p_env AND g.database_principal=session_user FOR SHARE OF g;
 IF NOT FOUND THEN RAISE EXCEPTION 'Telegram cancellation denied' USING ERRCODE='42501'; END IF;
 PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(pg_catalog.jsonb_build_array('telegram-predispatch-cancel',p_env,p_request)::text,0));
 SELECT * INTO r FROM public.novu_rust_telegram_predispatch_cancellations WHERE environment_id=p_env AND request_id=p_request;
 IF FOUND THEN
  IF (r.job_id,r.failure_revision,r.reason,r.database_principal,r.principal_oid) IS DISTINCT FROM (p_job,p_revision,p_reason,session_user,v_oid) THEN RAISE EXCEPTION 'Telegram cancellation conflict' USING ERRCODE='P4090'; END IF;
  RETURN jsonb_build_object('recorded',true,'replayed',true,'requestId',p_request);
 END IF;
 SELECT j.failure_revision INTO v_revision FROM public.novu_queue_jobs j
  WHERE j.environment_id=p_env AND j.id=p_job AND j.kind IN ('experimental_telegram','experimental_telegram_workflow') AND j.status='failed' FOR UPDATE OF j;
 IF NOT FOUND OR v_revision<>p_revision OR EXISTS(SELECT 1 FROM public.novu_rust_telegram_attempts a WHERE a.job_id=p_job AND a.environment_id=p_env) THEN
  RAISE EXCEPTION 'stale or ineligible Telegram failure' USING ERRCODE='22023';
 END IF;
 INSERT INTO public.novu_rust_telegram_predispatch_cancellations(environment_id,request_id,job_id,failure_revision,reason,database_principal,principal_oid)
 VALUES(p_env,p_request,p_job,p_revision,p_reason,session_user,v_oid);
 UPDATE public.novu_queue_jobs SET status='cancelled',last_error='Telegram pre-dispatch cancelled by operator',lease_token=NULL,lease_until=NULL WHERE environment_id=p_env AND id=p_job;
 RETURN jsonb_build_object('recorded',true,'replayed',false,'requestId',p_request);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_telegram_predispatch_cancel(text,uuid,uuid,bigint,text) FROM PUBLIC;
