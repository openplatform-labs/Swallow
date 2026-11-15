-- Separate decision grants from Slack result/list access.
CREATE TABLE public.novu_rust_operator_jwt_slack_decision_grants (
 issuer text NOT NULL, subject text NOT NULL, client_id text NOT NULL, environment_id text NOT NULL,
 decision text NOT NULL CHECK(decision IN ('retry','accepted')),
 PRIMARY KEY(issuer,subject,client_id,environment_id,decision),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
CREATE TABLE public.novu_rust_operator_jwt_slack_decision_requests (
 issuer text NOT NULL, subject text NOT NULL, client_id text NOT NULL, environment_id text NOT NULL,
 request_id uuid NOT NULL, job_id uuid NOT NULL, attempt_id uuid NOT NULL,
 decision text NOT NULL CHECK(decision IN ('retry','accepted')), reason text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(issuer,subject,client_id,environment_id,request_id)
);
REVOKE ALL ON public.novu_rust_operator_jwt_slack_decision_grants,public.novu_rust_operator_jwt_slack_decision_requests FROM PUBLIC;

CREATE FUNCTION public.novu_rust_operator_jwt_slack_resolve(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid,p_job uuid,p_attempt uuid,p_decision text,p_reason text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_count integer; v_existing public.novu_rust_operator_jwt_slack_decision_requests%ROWTYPE; v_operator text;
BEGIN
 IF p_request IS NULL OR p_job IS NULL OR p_attempt IS NULL OR p_decision IS NULL OR p_decision NOT IN ('retry','accepted') OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN
   RAISE EXCEPTION 'invalid request' USING ERRCODE='22023';
 END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_slack_decision_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env AND g.decision=p_decision FOR SHARE OF g;
 IF NOT FOUND OR NOT public.novu_rust_lock_operator_grant(p_env,'slack_'||p_decision) OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 INSERT INTO public.novu_rust_operator_jwt_slack_decision_requests(issuer,subject,client_id,environment_id,request_id,job_id,attempt_id,decision,reason)
 VALUES(p_issuer,p_subject,p_client,p_env,p_request,p_job,p_attempt,p_decision,p_reason) ON CONFLICT DO NOTHING;
 GET DIAGNOSTICS v_count=ROW_COUNT;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF v_count=0 THEN
   SELECT * INTO v_existing FROM public.novu_rust_operator_jwt_slack_decision_requests r WHERE r.issuer=p_issuer AND r.subject=p_subject AND r.client_id=p_client AND r.environment_id=p_env AND r.request_id=p_request;
   IF NOT FOUND OR v_existing.job_id IS DISTINCT FROM p_job OR v_existing.attempt_id IS DISTINCT FROM p_attempt OR v_existing.decision IS DISTINCT FROM p_decision OR v_existing.reason IS DISTINCT FROM p_reason THEN RAISE EXCEPTION 'request id conflict' USING ERRCODE='P4090'; END IF;
   RETURN true;
 END IF;
 PERFORM 1 FROM public.novu_queue_jobs j JOIN public.novu_rust_slack_attempts a ON a.job_id=j.id AND a.environment_id=j.environment_id WHERE j.id=p_job AND j.environment_id=p_env FOR UPDATE OF j,a;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 v_operator:='jwt:'||encode(sha256(convert_to(jsonb_build_array(p_issuer,p_subject,p_client,p_env)::text,'UTF8')),'hex');
 PERFORM public.novu_rust_resolve_slack_v2(p_env,p_job,p_attempt,p_decision,v_operator,p_reason);
 RETURN false;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_slack_resolve(text,text,text,text,bigint,uuid,uuid,uuid,text,text) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_operator_jwt_slack_decision_request(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_record public.novu_rust_operator_jwt_slack_decision_requests%ROWTYPE;
BEGIN
 IF p_request IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_slack_decision_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 SELECT * INTO v_record FROM public.novu_rust_operator_jwt_slack_decision_requests r WHERE r.issuer=p_issuer AND r.subject=p_subject AND r.client_id=p_client AND r.environment_id=p_env AND r.request_id=p_request;
 IF NOT FOUND THEN RETURN jsonb_build_object('found',false); END IF;
 PERFORM 1 FROM public.novu_rust_operator_jwt_slack_decision_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env AND g.decision=v_record.decision FOR SHARE OF g;
 IF NOT FOUND OR NOT public.novu_rust_lock_operator_grant(p_env,'slack_'||v_record.decision) OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('found',true,'requestId',v_record.request_id,'jobId',v_record.job_id,'attemptId',v_record.attempt_id,'decision',v_record.decision,'reason',v_record.reason,'createdAt',v_record.created_at);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_slack_decision_request(text,text,text,text,bigint,uuid) FROM PUBLIC;
