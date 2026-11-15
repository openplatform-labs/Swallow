-- JWT decisions are separate from review and DB login permissions.
CREATE TABLE public.novu_rust_operator_jwt_smtp_resolution_grants (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,
 can_accept boolean NOT NULL DEFAULT false,can_retry boolean NOT NULL DEFAULT false,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
CREATE TABLE public.novu_rust_operator_jwt_smtp_resolution_requests (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,request_id uuid NOT NULL,
 job_id uuid NOT NULL,attempt_revision uuid NOT NULL,failure_revision bigint NOT NULL,decision text NOT NULL,reason text NOT NULL,recorded boolean,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(issuer,subject,client_id,environment_id,request_id)
);
REVOKE ALL ON public.novu_rust_operator_jwt_smtp_resolution_grants,public.novu_rust_operator_jwt_smtp_resolution_requests FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_smtp_resolution_access(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_decision text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE permitted boolean;
BEGIN
 IF p_decision IS NULL OR p_decision NOT IN ('accepted','retry') THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 SELECT CASE WHEN p_decision='accepted' THEN g.can_accept ELSE g.can_retry END INTO permitted FROM public.novu_rust_operator_jwt_smtp_resolution_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT coalesce(permitted,false) OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 PERFORM 1 FROM public.novu_rust_smtp_resolvers g JOIN pg_catalog.pg_roles r ON r.oid=g.principal_oid AND r.rolname=g.database_principal AND r.rolcanlogin WHERE g.environment_id=p_env AND g.database_principal=session_user AND CASE WHEN p_decision='accepted' THEN g.can_accept ELSE g.can_retry END FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_smtp_resolution_access(text,text,text,text,bigint,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_smtp_resolve(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid,p_job uuid,p_attempt uuid,p_failure bigint,p_decision text,p_reason text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE n integer; r public.novu_rust_operator_jwt_smtp_resolution_requests%ROWTYPE; v_recorded boolean; v_db_request uuid;
BEGIN
 IF p_request IS NULL OR p_job IS NULL OR p_attempt IS NULL OR p_failure IS NULL OR p_failure<0 OR p_reason IS NULL OR octet_length(btrim(p_reason)) NOT BETWEEN 1 AND 512 THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_smtp_resolution_access(p_issuer,p_subject,p_client,p_env,p_exp,p_decision);
 INSERT INTO public.novu_rust_operator_jwt_smtp_resolution_requests(issuer,subject,client_id,environment_id,request_id,job_id,attempt_revision,failure_revision,decision,reason) VALUES(p_issuer,p_subject,p_client,p_env,p_request,p_job,p_attempt,p_failure,p_decision,p_reason) ON CONFLICT DO NOTHING;
 GET DIAGNOSTICS n=ROW_COUNT;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF n=0 THEN
  SELECT * INTO r FROM public.novu_rust_operator_jwt_smtp_resolution_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
  IF NOT FOUND OR (r.job_id,r.attempt_revision,r.failure_revision,r.decision,r.reason) IS DISTINCT FROM (p_job,p_attempt,p_failure,p_decision,p_reason) OR r.recorded IS NULL THEN RAISE EXCEPTION 'request id conflict' USING ERRCODE='P4090'; END IF;
  RETURN jsonb_build_object('recorded',r.recorded,'replayed',true);
 END IF;
 -- Isolate request IDs between JWT identities sharing a database login.
 v_db_request:=substr(encode(sha256(convert_to(jsonb_build_array('smtp-jwt-resolution-v1',p_issuer,p_subject,p_client,p_env,p_request)::text,'UTF8')),'hex'),1,32)::uuid;
 BEGIN
  v_recorded:=public.novu_rust_resolve_smtp(p_env,v_db_request,p_job,p_attempt,p_failure,p_decision,p_reason);
 EXCEPTION WHEN SQLSTATE '22023' THEN RAISE EXCEPTION 'resolution conflict' USING ERRCODE='P4090'; END;
 IF NOT v_recorded THEN RAISE EXCEPTION 'resolution request collision' USING ERRCODE='P4090'; END IF;
 UPDATE public.novu_rust_operator_jwt_smtp_resolution_requests SET recorded=v_recorded WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('recorded',v_recorded,'replayed',false);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_smtp_resolve(text,text,text,text,bigint,uuid,uuid,uuid,bigint,text,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_smtp_resolution_request(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid,p_decision text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_smtp_resolution_requests%ROWTYPE;
BEGIN
 IF p_request IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_smtp_resolution_access(p_issuer,p_subject,p_client,p_env,p_exp,p_decision);
 SELECT * INTO r FROM public.novu_rust_operator_jwt_smtp_resolution_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request AND decision=p_decision;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF NOT FOUND THEN RETURN jsonb_build_object('found',false); END IF;
 RETURN jsonb_build_object('found',true,'requestId',r.request_id,'jobId',r.job_id,'attemptRevision',r.attempt_revision,'failureRevision',r.failure_revision::text,'decision',r.decision,'reason',r.reason,'recorded',r.recorded,'createdAt',r.created_at::text);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_smtp_resolution_request(text,text,text,text,bigint,uuid,text) FROM PUBLIC;
