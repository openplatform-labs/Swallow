-- Retry modes are independently provisioned from result, list and policy access.
CREATE TABLE public.novu_rust_operator_jwt_resend_429_grants (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,mode text NOT NULL CHECK(mode IN ('immediate','after')),
 PRIMARY KEY(issuer,subject,client_id,environment_id,mode),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
CREATE TABLE public.novu_rust_operator_jwt_resend_429_requests (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,request_id uuid NOT NULL,
 job_id uuid NOT NULL,attempt_id uuid NOT NULL,mode text NOT NULL CHECK(mode IN ('immediate','after')),reason text NOT NULL,available_at timestamptz,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(issuer,subject,client_id,environment_id,request_id)
);
CREATE TABLE public.novu_rust_operator_jwt_resend_429_audit (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),environment_id text NOT NULL,actor_issuer text NOT NULL,actor_subject text NOT NULL,actor_client text NOT NULL,
 database_principal name NOT NULL,request_id uuid NOT NULL,job_id uuid NOT NULL,attempt_id uuid NOT NULL,mode text NOT NULL,reason text NOT NULL,available_at timestamptz NOT NULL,recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE public.novu_rust_operator_jwt_resend_429_audit_grants (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
CREATE INDEX novu_rust_jwt_resend_429_audit_page ON public.novu_rust_operator_jwt_resend_429_audit(environment_id,id);
REVOKE ALL ON public.novu_rust_operator_jwt_resend_429_grants,public.novu_rust_operator_jwt_resend_429_requests,public.novu_rust_operator_jwt_resend_429_audit,public.novu_rust_operator_jwt_resend_429_audit_grants FROM PUBLIC;

CREATE FUNCTION public.novu_rust_operator_jwt_resend_429_resolve(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid,p_job uuid,p_attempt uuid,p_mode text,p_reason text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_count integer; r public.novu_rust_operator_jwt_resend_429_requests%ROWTYPE; v_current uuid; v_operator text; v_available timestamptz;
BEGIN
 IF p_request IS NULL OR p_job IS NULL OR p_attempt IS NULL OR p_mode IS NULL OR p_mode NOT IN ('immediate','after') OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_resend_429_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env AND g.mode=p_mode FOR SHARE OF g;
 IF NOT FOUND OR NOT public.novu_rust_lock_operator_grant(p_env,'retry') OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 INSERT INTO public.novu_rust_operator_jwt_resend_429_requests(issuer,subject,client_id,environment_id,request_id,job_id,attempt_id,mode,reason) VALUES(p_issuer,p_subject,p_client,p_env,p_request,p_job,p_attempt,p_mode,p_reason) ON CONFLICT DO NOTHING;
 GET DIAGNOSTICS v_count=ROW_COUNT;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF v_count=0 THEN
  SELECT * INTO r FROM public.novu_rust_operator_jwt_resend_429_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
  IF NOT FOUND OR (r.job_id,r.attempt_id,r.mode,r.reason) IS DISTINCT FROM (p_job,p_attempt,p_mode,p_reason) OR r.available_at IS NULL THEN RAISE EXCEPTION 'request conflict' USING ERRCODE='P4090'; END IF;
  IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
  RETURN jsonb_build_object('requestId',p_request,'replayed',true,'availableAt',r.available_at::text);
 END IF;
 SELECT a.attempt_id INTO v_current FROM public.novu_queue_jobs j JOIN public.novu_rust_resend_attempts a ON a.job_id=j.id AND a.environment_id=j.environment_id
  WHERE j.environment_id=p_env AND j.id=p_job AND j.kind='experimental_resend_email' AND j.status='failed' AND a.state='rejected' AND a.http_status=429 FOR UPDATE OF j,a;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF NOT FOUND OR v_current IS DISTINCT FROM p_attempt THEN RAISE EXCEPTION 'attempt changed or unavailable' USING ERRCODE='P4090'; END IF;
 v_operator:='jwt:'||encode(sha256(convert_to(jsonb_build_array(p_issuer,p_subject,p_client,p_env)::text,'UTF8')),'hex');
 IF p_mode='after' THEN
  v_available:=public.novu_rust_resolve_resend_after(p_env,p_job,v_operator,p_reason);
 ELSE
  PERFORM public.novu_rust_resolve_email_sms(p_env,p_job,'retry',v_operator,p_reason);
  SELECT available_at INTO v_available FROM public.novu_queue_jobs WHERE environment_id=p_env AND id=p_job;
 END IF;
 IF v_available IS NULL THEN RAISE EXCEPTION 'missing resolution record' USING ERRCODE='55000'; END IF;
 INSERT INTO public.novu_rust_operator_jwt_resend_429_audit(environment_id,actor_issuer,actor_subject,actor_client,database_principal,request_id,job_id,attempt_id,mode,reason,available_at) VALUES(p_env,p_issuer,p_subject,p_client,session_user,p_request,p_job,p_attempt,p_mode,p_reason,v_available);
 UPDATE public.novu_rust_operator_jwt_resend_429_requests SET available_at=v_available WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('requestId',p_request,'replayed',false,'availableAt',v_available::text);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_resend_429_resolve(text,text,text,text,bigint,uuid,uuid,uuid,text,text) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_operator_jwt_resend_429_request(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_resend_429_requests%ROWTYPE;
BEGIN
 IF p_request IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_resend_429_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND OR NOT public.novu_rust_lock_operator_grant(p_env,'retry') OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 SELECT * INTO r FROM public.novu_rust_operator_jwt_resend_429_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF NOT FOUND THEN RETURN jsonb_build_object('found',false); END IF;
 PERFORM 1 FROM public.novu_rust_operator_jwt_resend_429_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env AND g.mode=r.mode FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('found',true,'requestId',r.request_id,'jobId',r.job_id,'attemptId',r.attempt_id,'mode',r.mode,'reason',r.reason,'availableAt',r.available_at::text,'createdAt',r.created_at::text);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_resend_429_request(text,text,text,text,bigint,uuid) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_operator_jwt_resend_429_audit_read(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_after uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_result jsonb;
BEGIN
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_resend_429_audit_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 WITH candidates AS MATERIALIZED (SELECT * FROM public.novu_rust_operator_jwt_resend_429_audit WHERE environment_id=p_env AND (p_after IS NULL OR id>p_after) ORDER BY id LIMIT 51), page AS MATERIALIZED (SELECT * FROM candidates ORDER BY id LIMIT 50)
 SELECT jsonb_build_object('data',coalesce((SELECT jsonb_agg(jsonb_build_object('id',id,'actorIssuer',actor_issuer,'actorSubject',actor_subject,'actorClientId',actor_client,'databasePrincipal',database_principal,'requestId',request_id,'jobId',job_id,'attemptId',attempt_id,'mode',mode,'reason',reason,'availableAt',available_at::text,'recordedAt',recorded_at::text) ORDER BY id) FROM page),'[]'::jsonb),'hasMore',(SELECT count(*)>50 FROM candidates),'next',CASE WHEN (SELECT count(*)>50 FROM candidates) THEN (SELECT id::text FROM page ORDER BY id DESC LIMIT 1) ELSE NULL END) INTO v_result;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN v_result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_resend_429_audit_read(text,text,text,text,bigint,uuid) FROM PUBLIC;
