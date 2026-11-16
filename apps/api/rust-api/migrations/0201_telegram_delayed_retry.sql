-- Extend existing Telegram retry authority and request identity to scheduled 429 retries.
CREATE OR REPLACE FUNCTION public.novu_rust_operator_jwt_telegram_resolve(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid,p_job uuid,p_attempt uuid,p_decision text,p_reason text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_telegram_resolution_requests%ROWTYPE; worker_request uuid; result jsonb;
BEGIN
 IF p_request IS NULL OR p_job IS NULL OR p_attempt IS NULL OR p_decision IS NULL OR p_decision NOT IN ('retry','accepted','retry_after') OR p_reason IS NULL OR octet_length(btrim(p_reason)) NOT BETWEEN 1 AND 512 THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_telegram_resolution_access(p_issuer,p_subject,p_client,p_env,p_exp,CASE WHEN p_decision='retry_after' THEN 'retry' ELSE p_decision END);
 PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(pg_catalog.jsonb_build_array('jwt-telegram-resolution',p_issuer,p_subject,p_client,p_env,p_request)::text,0));
 SELECT * INTO r FROM public.novu_rust_operator_jwt_telegram_resolution_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
 IF FOUND THEN
  IF (r.job_id,r.attempt_id,r.decision,r.reason) IS DISTINCT FROM (p_job,p_attempt,p_decision,p_reason) THEN RAISE EXCEPTION 'request conflict' USING ERRCODE='P4090'; END IF;
  IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
  RETURN jsonb_build_object('requestId',p_request,'recorded',true,'replayed',true);
 END IF;
 worker_request:=gen_random_uuid();
 IF p_decision='retry_after' THEN
  result:=public.novu_rust_telegram_resolve_after(p_env,worker_request,p_job,p_attempt,p_reason);
 ELSE
  result:=public.novu_rust_telegram_resolve(p_env,worker_request,p_job,p_attempt,p_decision,p_reason);
 END IF;
 INSERT INTO public.novu_rust_operator_jwt_telegram_resolution_requests(issuer,subject,client_id,environment_id,request_id,job_id,attempt_id,decision,reason,worker_request_id) VALUES(p_issuer,p_subject,p_client,p_env,p_request,p_job,p_attempt,p_decision,p_reason,worker_request);
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('requestId',p_request,'recorded',true,'replayed',false);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_telegram_resolve(text,text,text,text,bigint,uuid,uuid,uuid,text,text) FROM PUBLIC;
CREATE OR REPLACE FUNCTION public.novu_rust_operator_jwt_telegram_resolution_request(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid,p_decision text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_telegram_resolution_requests%ROWTYPE;
BEGIN
 IF p_request IS NULL OR p_decision IS NULL OR p_decision NOT IN ('retry','accepted','retry_after') THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_telegram_resolution_access(p_issuer,p_subject,p_client,p_env,p_exp,CASE WHEN p_decision='retry_after' THEN 'retry' ELSE p_decision END);
 SELECT * INTO r FROM public.novu_rust_operator_jwt_telegram_resolution_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request AND decision=p_decision;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF NOT FOUND THEN RETURN jsonb_build_object('found',false); END IF;
 RETURN jsonb_build_object('found',true,'requestId',r.request_id,'jobId',r.job_id,'attemptId',r.attempt_id,'decision',r.decision,'reason',r.reason,'recorded',true,'createdAt',r.created_at::text);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_telegram_resolution_request(text,text,text,text,bigint,uuid,text) FROM PUBLIC;
