-- Independent JWT permissions; API database LOGIN also needs corresponding Telegram operator grants.
CREATE TABLE public.novu_rust_operator_jwt_telegram_resolution_grants (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,
 can_list boolean NOT NULL DEFAULT false,can_retry boolean NOT NULL DEFAULT false,can_accept boolean NOT NULL DEFAULT false,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
CREATE TABLE public.novu_rust_operator_jwt_telegram_resolution_requests (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL REFERENCES public.novu_rust_environments(id) ON DELETE CASCADE,
 request_id uuid NOT NULL,job_id uuid NOT NULL,attempt_id uuid NOT NULL,decision text NOT NULL,reason text NOT NULL,worker_request_id uuid NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(issuer,subject,client_id,environment_id,request_id)
);
REVOKE ALL ON public.novu_rust_operator_jwt_telegram_resolution_grants,public.novu_rust_operator_jwt_telegram_resolution_requests FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_telegram_resolution_access(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_action text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE allowed boolean;
BEGIN
 IF p_action IS NULL OR p_action NOT IN ('list','retry','accepted') THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 SELECT CASE p_action WHEN 'list' THEN g.can_list WHEN 'retry' THEN g.can_retry ELSE g.can_accept END INTO allowed FROM public.novu_rust_operator_jwt_telegram_resolution_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT COALESCE(allowed,false) OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 PERFORM public.novu_rust_telegram_operator_access(p_env,CASE WHEN p_action='list' THEN 'view' ELSE p_action END);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_telegram_resolution_access(text,text,text,text,bigint,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_telegram_attention(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_after uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb;
BEGIN
 PERFORM public.novu_rust_operator_jwt_telegram_resolution_access(p_issuer,p_subject,p_client,p_env,p_exp,'list');
 result:=public.novu_rust_telegram_attention(p_env,p_after);
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_telegram_attention(text,text,text,text,bigint,uuid) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_telegram_resolve(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid,p_job uuid,p_attempt uuid,p_decision text,p_reason text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_telegram_resolution_requests%ROWTYPE; worker_request uuid; result jsonb;
BEGIN
 IF p_request IS NULL OR p_job IS NULL OR p_attempt IS NULL OR p_decision IS NULL OR p_decision NOT IN ('retry','accepted') OR p_reason IS NULL OR octet_length(btrim(p_reason)) NOT BETWEEN 1 AND 512 THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_telegram_resolution_access(p_issuer,p_subject,p_client,p_env,p_exp,p_decision);
 PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(pg_catalog.jsonb_build_array('jwt-telegram-resolution',p_issuer,p_subject,p_client,p_env,p_request)::text,0));
 SELECT * INTO r FROM public.novu_rust_operator_jwt_telegram_resolution_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
 IF FOUND THEN
  IF (r.job_id,r.attempt_id,r.decision,r.reason) IS DISTINCT FROM (p_job,p_attempt,p_decision,p_reason) THEN RAISE EXCEPTION 'request conflict' USING ERRCODE='P4090'; END IF;
  IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
  RETURN jsonb_build_object('requestId',p_request,'recorded',true,'replayed',true);
 END IF;
 worker_request:=gen_random_uuid();
 result:=public.novu_rust_telegram_resolve(p_env,worker_request,p_job,p_attempt,p_decision,p_reason);
 INSERT INTO public.novu_rust_operator_jwt_telegram_resolution_requests(issuer,subject,client_id,environment_id,request_id,job_id,attempt_id,decision,reason,worker_request_id) VALUES(p_issuer,p_subject,p_client,p_env,p_request,p_job,p_attempt,p_decision,p_reason,worker_request);
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('requestId',p_request,'recorded',true,'replayed',false);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_telegram_resolve(text,text,text,text,bigint,uuid,uuid,uuid,text,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_telegram_resolution_request(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid,p_decision text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_telegram_resolution_requests%ROWTYPE;
BEGIN
 IF p_request IS NULL OR p_decision IS NULL OR p_decision NOT IN ('retry','accepted') THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_telegram_resolution_access(p_issuer,p_subject,p_client,p_env,p_exp,p_decision);
 SELECT * INTO r FROM public.novu_rust_operator_jwt_telegram_resolution_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request AND decision=p_decision;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF NOT FOUND THEN RETURN jsonb_build_object('found',false); END IF;
 RETURN jsonb_build_object('found',true,'requestId',r.request_id,'jobId',r.job_id,'attemptId',r.attempt_id,'decision',r.decision,'reason',r.reason,'recorded',true,'createdAt',r.created_at::text);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_telegram_resolution_request(text,text,text,text,bigint,uuid,text) FROM PUBLIC;
