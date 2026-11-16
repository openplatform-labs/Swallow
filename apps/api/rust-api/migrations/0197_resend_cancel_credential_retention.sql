-- Independent, DBA-provisioned JWT right; database login authorization is separate.
CREATE TABLE public.novu_rust_operator_jwt_cancel_cred_retention_grants (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
CREATE TABLE public.novu_rust_operator_jwt_cancel_cred_retention_requests (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,
 request_id uuid NOT NULL,worker_request_id uuid NOT NULL,older_than_seconds integer NOT NULL,batch_limit integer NOT NULL,reason text NOT NULL,
 deleted_count bigint NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(issuer,subject,client_id,environment_id,request_id),UNIQUE(environment_id,worker_request_id)
);
REVOKE ALL ON public.novu_rust_operator_jwt_cancel_cred_retention_grants,public.novu_rust_operator_jwt_cancel_cred_retention_requests FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_cancel_cred_retention_access(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_cancel_cred_retention_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 PERFORM public.novu_rust_resend_cancel_cred_retention_request_access(p_env);
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_cancel_cred_retention_access(text,text,text,text,bigint) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_cancel_cred_retention_submit(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid,p_age integer,p_limit integer,p_reason text) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE old public.novu_rust_operator_jwt_cancel_cred_retention_requests%ROWTYPE; worker_id uuid; result jsonb;
BEGIN
 IF p_request IS NULL OR p_age IS NULL OR p_age NOT BETWEEN 86401 AND 31536000 OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 1000 OR p_reason IS NULL OR btrim(p_reason)='' OR octet_length(p_reason)>500 OR p_reason ~ '[[:cntrl:]]' THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_cancel_cred_retention_access(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(pg_catalog.jsonb_build_array('cancel-cred-retention',p_env,p_issuer,p_subject,p_client,p_request)::text,0));
 SELECT * INTO old FROM public.novu_rust_operator_jwt_cancel_cred_retention_requests WHERE environment_id=p_env AND issuer=p_issuer AND subject=p_subject AND client_id=p_client AND request_id=p_request;
 IF FOUND THEN
  IF (old.older_than_seconds,old.batch_limit,old.reason) IS DISTINCT FROM (p_age,p_limit,p_reason) THEN RAISE EXCEPTION 'request conflict' USING ERRCODE='P4090'; END IF;
  IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
  RETURN jsonb_build_object('requestId',p_request,'deleted',old.deleted_count,'replayed',true);
 END IF;
 worker_id:=substr(encode(sha256(convert_to(jsonb_build_array('cancel-cred-retention-v1',p_env,p_issuer,p_subject,p_client,p_request)::text,'UTF8')),'hex'),1,32)::uuid;
 result:=public.novu_rust_resend_cancel_cred_retention_submit(p_env,worker_id,p_age,p_limit,p_reason);
 INSERT INTO public.novu_rust_operator_jwt_cancel_cred_retention_requests(issuer,subject,client_id,environment_id,request_id,worker_request_id,older_than_seconds,batch_limit,reason,deleted_count) VALUES(p_issuer,p_subject,p_client,p_env,p_request,worker_id,p_age,p_limit,p_reason,(result->>'deleted')::bigint);
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('requestId',p_request,'deleted',(result->>'deleted')::bigint,'replayed',false);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_cancel_cred_retention_submit(text,text,text,text,bigint,uuid,integer,integer,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_cancel_cred_retention_get(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_cancel_cred_retention_requests%ROWTYPE;
BEGIN
 IF p_request IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_cancel_cred_retention_access(p_issuer,p_subject,p_client,p_env,p_exp);
 SELECT * INTO r FROM public.novu_rust_operator_jwt_cancel_cred_retention_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF NOT FOUND THEN RETURN jsonb_build_object('found',false); END IF;
 RETURN jsonb_build_object('found',true,'requestId',p_request,'olderThanSeconds',r.older_than_seconds,'batchLimit',r.batch_limit,'reason',r.reason,'deleted',r.deleted_count,'createdAt',r.created_at);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_cancel_cred_retention_get(text,text,text,text,bigint,uuid) FROM PUBLIC;
