-- Request IDs deduplicate exact JWT workflow changes without storing template bodies twice.
CREATE TABLE public.novu_rust_operator_jwt_workflow_requests (
 issuer text NOT NULL, subject text NOT NULL, client_id text NOT NULL, environment_id text NOT NULL,
 request_id uuid NOT NULL, workflow_identifier text NOT NULL, resource text NOT NULL,
 request_digest bytea NOT NULL CHECK (octet_length(request_digest)=32), reason text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(issuer,subject,client_id,environment_id,request_id)
);
REVOKE ALL ON public.novu_rust_operator_jwt_workflow_requests FROM PUBLIC;

CREATE FUNCTION public.novu_rust_operator_jwt_workflow_request_access(
 p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_resource text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 IF p_resource IS NULL OR p_resource NOT IN ('email','sms','push','slack','email-provider') THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM 1 FROM public.novu_rust_operator_jwt_workflow_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env AND g.resource=p_resource AND g.permission='write' FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_workflow_request_access(text,text,text,text,bigint,text) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_operator_jwt_workflow_write_v2(
 p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid,p_workflow text,p_resource text,p_expected jsonb,p_desired jsonb,p_reason text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_existing public.novu_rust_operator_jwt_workflow_requests%ROWTYPE; v_digest bytea;
BEGIN
 IF p_request IS NULL OR p_workflow IS NULL OR octet_length(p_workflow) NOT BETWEEN 1 AND 512 OR p_reason IS NULL OR octet_length(btrim(p_reason)) NOT BETWEEN 1 AND 1000 OR NOT public.novu_rust_operator_jwt_workflow_value_valid(p_resource,p_expected) OR NOT public.novu_rust_operator_jwt_workflow_value_valid(p_resource,p_desired) THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_workflow_request_access(p_issuer,p_subject,p_client,p_env,p_exp,p_resource);
 v_digest:=pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.jsonb_build_array(p_workflow,p_resource,p_expected,p_desired,p_reason)::text,'UTF8'));
 PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(pg_catalog.jsonb_build_array('jwt-workflow-request',p_issuer,p_subject,p_client,p_env,p_request)::text,0));
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 SELECT * INTO v_existing FROM public.novu_rust_operator_jwt_workflow_requests r WHERE r.issuer=p_issuer AND r.subject=p_subject AND r.client_id=p_client AND r.environment_id=p_env AND r.request_id=p_request;
 IF FOUND THEN
  IF v_existing.request_digest IS DISTINCT FROM v_digest THEN RAISE EXCEPTION 'request conflict' USING ERRCODE='P4090'; END IF;
  IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
  RETURN jsonb_build_object('recorded',true,'replayed',true,'requestId',p_request);
 END IF;
 PERFORM public.novu_rust_operator_jwt_workflow_write(p_issuer,p_subject,p_client,p_env,p_exp,p_workflow,p_resource,p_expected,p_desired,p_reason);
 INSERT INTO public.novu_rust_operator_jwt_workflow_requests(issuer,subject,client_id,environment_id,request_id,workflow_identifier,resource,request_digest,reason)
 VALUES(p_issuer,p_subject,p_client,p_env,p_request,p_workflow,p_resource,v_digest,btrim(p_reason));
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('recorded',true,'replayed',false,'requestId',p_request);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_workflow_write_v2(text,text,text,text,bigint,uuid,text,text,jsonb,jsonb,text) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_operator_jwt_workflow_request_read(
 p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_resource text,p_request uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_workflow_requests%ROWTYPE;
BEGIN
 IF p_request IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_workflow_request_access(p_issuer,p_subject,p_client,p_env,p_exp,p_resource);
 SELECT * INTO r FROM public.novu_rust_operator_jwt_workflow_requests w WHERE w.issuer=p_issuer AND w.subject=p_subject AND w.client_id=p_client AND w.environment_id=p_env AND w.request_id=p_request AND w.resource=p_resource;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF NOT FOUND THEN RETURN jsonb_build_object('found',false); END IF;
 RETURN jsonb_build_object('found',true,'requestId',r.request_id,'workflow',r.workflow_identifier,'resource',r.resource,'reason',r.reason,'createdAt',r.created_at);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_workflow_request_read(text,text,text,text,bigint,text,uuid) FROM PUBLIC;
