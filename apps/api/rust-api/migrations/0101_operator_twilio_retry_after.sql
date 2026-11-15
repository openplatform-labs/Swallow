-- Delayed Twilio retry decisions have an independent JWT grant and request namespace.
CREATE TABLE public.novu_rust_operator_jwt_twilio_retry_grants (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
CREATE TABLE public.novu_rust_operator_jwt_twilio_retry_requests (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,request_id uuid NOT NULL,
 job_id uuid NOT NULL,attempt_id uuid NOT NULL,reason text NOT NULL,available_at timestamptz,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(issuer,subject,client_id,environment_id,request_id)
);
REVOKE ALL ON public.novu_rust_operator_jwt_twilio_retry_grants,public.novu_rust_operator_jwt_twilio_retry_requests FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_twilio_retry_after(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid,p_job uuid,p_attempt uuid,p_decision text,p_reason text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_count integer; r public.novu_rust_operator_jwt_twilio_retry_requests%ROWTYPE; v_operator text; v_available timestamptz;
BEGIN
 IF p_request IS NULL OR p_job IS NULL OR p_attempt IS NULL OR p_decision IS DISTINCT FROM 'retry-after' OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_twilio_retry_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND OR NOT public.novu_rust_lock_operator_grant(p_env,'retry') OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 INSERT INTO public.novu_rust_operator_jwt_twilio_retry_requests(issuer,subject,client_id,environment_id,request_id,job_id,attempt_id,reason) VALUES(p_issuer,p_subject,p_client,p_env,p_request,p_job,p_attempt,p_reason) ON CONFLICT DO NOTHING;
 GET DIAGNOSTICS v_count=ROW_COUNT;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF v_count=0 THEN
  SELECT * INTO r FROM public.novu_rust_operator_jwt_twilio_retry_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
  IF NOT FOUND OR (r.job_id,r.attempt_id,r.reason) IS DISTINCT FROM (p_job,p_attempt,p_reason) OR r.available_at IS NULL THEN RAISE EXCEPTION 'request id conflict' USING ERRCODE='P4090'; END IF;
  IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
  RETURN jsonb_build_object('recorded',true,'replayed',true,'availableAt',r.available_at::text);
 END IF;
 PERFORM 1 FROM public.novu_queue_jobs j JOIN public.novu_rust_twilio_attempts a ON a.job_id=j.id AND a.environment_id=j.environment_id WHERE j.id=p_job AND j.environment_id=p_env FOR UPDATE OF j,a;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 v_operator:='jwt:'||encode(sha256(convert_to(jsonb_build_array(p_issuer,p_subject,p_client,p_env)::text,'UTF8')),'hex');
 v_available:=public.novu_rust_resolve_twilio_after(p_env,p_job,p_attempt,v_operator,p_reason);
 UPDATE public.novu_rust_operator_jwt_twilio_retry_requests SET available_at=v_available WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('recorded',true,'replayed',false,'availableAt',v_available::text);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_twilio_retry_after(text,text,text,text,bigint,uuid,uuid,uuid,text,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_twilio_retry_request(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_twilio_retry_requests%ROWTYPE;
BEGIN
 IF p_request IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_twilio_retry_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND OR NOT public.novu_rust_lock_operator_grant(p_env,'retry') OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 SELECT * INTO r FROM public.novu_rust_operator_jwt_twilio_retry_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF NOT FOUND THEN RETURN jsonb_build_object('found',false); END IF;
 RETURN jsonb_build_object('found',true,'requestId',r.request_id,'jobId',r.job_id,'attemptId',r.attempt_id,'decision','retry-after','reason',r.reason,'availableAt',r.available_at::text,'createdAt',r.created_at::text);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_twilio_retry_request(text,text,text,text,bigint,uuid) FROM PUBLIC;
