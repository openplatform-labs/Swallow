-- The same request ID namespace covers immediate and delayed Slack decisions.
ALTER TABLE public.novu_rust_operator_jwt_slack_decision_requests DROP CONSTRAINT novu_rust_operator_jwt_slack_decision_requests_decision_check;
ALTER TABLE public.novu_rust_operator_jwt_slack_decision_requests ADD CONSTRAINT novu_rust_operator_jwt_slack_decision_requests_decision_check CHECK (decision IN ('retry','accepted','retry-after'));
ALTER TABLE public.novu_rust_operator_jwt_slack_decision_requests ADD COLUMN available_at timestamptz;
CREATE FUNCTION public.novu_rust_operator_jwt_slack_retry_after(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid,p_job uuid,p_attempt uuid,p_decision text,p_reason text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_count integer; r public.novu_rust_operator_jwt_slack_decision_requests%ROWTYPE; v_operator text; v_available timestamptz;
BEGIN
 IF p_request IS NULL OR p_job IS NULL OR p_attempt IS NULL OR p_decision IS DISTINCT FROM 'retry-after' OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_slack_decision_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env AND g.decision='retry' FOR SHARE OF g;
 IF NOT FOUND OR NOT public.novu_rust_lock_operator_grant(p_env,'slack_retry') OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 INSERT INTO public.novu_rust_operator_jwt_slack_decision_requests(issuer,subject,client_id,environment_id,request_id,job_id,attempt_id,decision,reason) VALUES(p_issuer,p_subject,p_client,p_env,p_request,p_job,p_attempt,p_decision,p_reason) ON CONFLICT DO NOTHING;
 GET DIAGNOSTICS v_count=ROW_COUNT;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF v_count=0 THEN
  SELECT * INTO r FROM public.novu_rust_operator_jwt_slack_decision_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
  IF NOT FOUND OR (r.job_id,r.attempt_id,r.decision,r.reason) IS DISTINCT FROM (p_job,p_attempt,p_decision,p_reason) OR r.available_at IS NULL THEN RAISE EXCEPTION 'request id conflict' USING ERRCODE='P4090'; END IF;
  IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
  RETURN jsonb_build_object('recorded',true,'replayed',true,'availableAt',r.available_at::text);
 END IF;
 PERFORM 1 FROM public.novu_queue_jobs j JOIN public.novu_rust_slack_attempts a ON a.job_id=j.id AND a.environment_id=j.environment_id WHERE j.id=p_job AND j.environment_id=p_env FOR UPDATE OF j,a;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 v_operator:='jwt:'||encode(sha256(convert_to(jsonb_build_array(p_issuer,p_subject,p_client,p_env)::text,'UTF8')),'hex');
 v_available:=public.novu_rust_resolve_slack_after(p_env,p_job,p_attempt,v_operator,p_reason);
 UPDATE public.novu_rust_operator_jwt_slack_decision_requests SET available_at=v_available WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('recorded',true,'replayed',false,'availableAt',v_available::text);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_slack_retry_after(text,text,text,text,bigint,uuid,uuid,uuid,text,text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_operator_jwt_slack_decision_request(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_record public.novu_rust_operator_jwt_slack_decision_requests%ROWTYPE; v_grant text;
BEGIN
 IF p_request IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_slack_decision_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 SELECT * INTO v_record FROM public.novu_rust_operator_jwt_slack_decision_requests r WHERE r.issuer=p_issuer AND r.subject=p_subject AND r.client_id=p_client AND r.environment_id=p_env AND r.request_id=p_request;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF NOT FOUND THEN RETURN jsonb_build_object('found',false); END IF;
 v_grant:=CASE WHEN v_record.decision='retry-after' THEN 'retry' ELSE v_record.decision END;
 PERFORM 1 FROM public.novu_rust_operator_jwt_slack_decision_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env AND g.decision=v_grant FOR SHARE OF g;
 IF NOT FOUND OR NOT public.novu_rust_lock_operator_grant(p_env,'slack_'||v_grant) OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('found',true,'requestId',v_record.request_id,'jobId',v_record.job_id,'attemptId',v_record.attempt_id,'decision',v_record.decision,'reason',v_record.reason,'availableAt',v_record.available_at::text,'createdAt',v_record.created_at);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_slack_decision_request(text,text,text,text,bigint,uuid) FROM PUBLIC;
