-- JWT permissions are independent of the API DB login's worker-policy permissions.
CREATE TABLE public.novu_rust_operator_jwt_resend_auto_policy_grants (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,
 can_read boolean NOT NULL DEFAULT false,can_write boolean NOT NULL DEFAULT false,can_audit boolean NOT NULL DEFAULT false,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
CREATE TABLE public.novu_rust_operator_jwt_resend_auto_policy_requests (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,request_id uuid NOT NULL,
 expected_revision bigint NOT NULL,enabled boolean NOT NULL,max_retries integer NOT NULL,cutoff text NOT NULL,reason text NOT NULL,
 policy jsonb NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(issuer,subject,client_id,environment_id,request_id)
);
CREATE TABLE public.novu_rust_operator_jwt_resend_auto_policy_audit (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),environment_id text NOT NULL,actor_issuer text NOT NULL,actor_subject text NOT NULL,actor_client text NOT NULL,
 database_principal name NOT NULL,request_id uuid NOT NULL,previous_policy jsonb,current_policy jsonb NOT NULL,reason text NOT NULL,recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX novu_rust_jwt_resend_auto_policy_audit_page ON public.novu_rust_operator_jwt_resend_auto_policy_audit(environment_id,id);
REVOKE ALL ON public.novu_rust_operator_jwt_resend_auto_policy_grants,public.novu_rust_operator_jwt_resend_auto_policy_requests,public.novu_rust_operator_jwt_resend_auto_policy_audit FROM PUBLIC;

CREATE FUNCTION public.novu_rust_operator_jwt_resend_auto_policy_access(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_access text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_access IS NULL OR p_access NOT IN ('read','write','audit') THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_resend_auto_policy_grants g
  WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env
   AND CASE p_access WHEN 'read' THEN g.can_read WHEN 'write' THEN g.can_write ELSE g.can_audit END FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF p_access IN ('read','write') THEN PERFORM public.novu_rust_resend_auto_policy_access(p_env,p_access); END IF;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_resend_auto_policy_access(text,text,text,text,bigint,text) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_operator_jwt_resend_auto_policy_read(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_result jsonb;
BEGIN
 PERFORM public.novu_rust_operator_jwt_resend_auto_policy_access(p_issuer,p_subject,p_client,p_env,p_exp,'read');
 v_result:=public.novu_rust_resend_auto_policy_read(p_env);
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN v_result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_resend_auto_policy_read(text,text,text,text,bigint) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_operator_jwt_resend_auto_policy_change(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid,p_expected bigint,p_enabled boolean,p_max integer,p_cutoff text,p_reason text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_resend_auto_policy_requests%ROWTYPE; v_worker_request uuid; v_result jsonb; v_previous jsonb;
BEGIN
 IF p_request IS NULL OR p_expected IS NULL OR p_expected < -1 OR p_enabled IS NULL OR p_max IS NULL OR p_max NOT BETWEEN 1 AND 10 OR p_cutoff IS NULL OR p_cutoff NOT IN ('now','preserve') OR p_reason IS NULL OR octet_length(btrim(p_reason)) NOT BETWEEN 1 AND 512 OR octet_length(p_reason)>512 THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_resend_auto_policy_access(p_issuer,p_subject,p_client,p_env,p_exp,'write');
 PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(pg_catalog.jsonb_build_array('jwt-resend-auto-policy',p_issuer,p_subject,p_client,p_env,p_request)::text,0));
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 SELECT * INTO r FROM public.novu_rust_operator_jwt_resend_auto_policy_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
 IF FOUND THEN
  IF (r.expected_revision,r.enabled,r.max_retries,r.cutoff,r.reason) IS DISTINCT FROM (p_expected,p_enabled,p_max,p_cutoff,p_reason) THEN RAISE EXCEPTION 'request conflict' USING ERRCODE='P4090'; END IF;
  IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
  RETURN jsonb_build_object('requestId',p_request,'replayed',true,'policy',r.policy);
 END IF;
 v_worker_request:=gen_random_uuid();
 BEGIN
  v_result:=public.novu_rust_resend_auto_policy_set_v2(p_env,v_worker_request,p_expected,p_enabled,p_max,p_cutoff,p_reason);
 EXCEPTION WHEN SQLSTATE '40001' THEN RAISE EXCEPTION 'stale policy revision' USING ERRCODE='P4090'; END;
 SELECT previous_policy INTO v_previous FROM public.novu_rust_resend_auto_policy_audit WHERE environment_id=p_env AND database_principal=session_user AND request_id=v_worker_request;
 IF NOT FOUND THEN RAISE EXCEPTION 'missing policy audit' USING ERRCODE='55000'; END IF;
 INSERT INTO public.novu_rust_operator_jwt_resend_auto_policy_audit(environment_id,actor_issuer,actor_subject,actor_client,database_principal,request_id,previous_policy,current_policy,reason) VALUES(p_env,p_issuer,p_subject,p_client,session_user,p_request,v_previous,v_result->'policy',p_reason);
 INSERT INTO public.novu_rust_operator_jwt_resend_auto_policy_requests(issuer,subject,client_id,environment_id,request_id,expected_revision,enabled,max_retries,cutoff,reason,policy) VALUES(p_issuer,p_subject,p_client,p_env,p_request,p_expected,p_enabled,p_max,p_cutoff,p_reason,v_result->'policy');
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('requestId',p_request,'replayed',false,'policy',v_result->'policy');
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_resend_auto_policy_change(text,text,text,text,bigint,uuid,bigint,boolean,integer,text,text) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_operator_jwt_resend_auto_policy_request(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_resend_auto_policy_requests%ROWTYPE;
BEGIN
 IF p_request IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_resend_auto_policy_access(p_issuer,p_subject,p_client,p_env,p_exp,'write');
 SELECT * INTO r FROM public.novu_rust_operator_jwt_resend_auto_policy_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF NOT FOUND THEN RETURN jsonb_build_object('found',false); END IF;
 RETURN jsonb_build_object('found',true,'requestId',r.request_id,'expectedRevision',r.expected_revision::text,'enabled',r.enabled,'maxRetries',r.max_retries,'cutoff',r.cutoff,'reason',r.reason,'policy',r.policy,'createdAt',r.created_at::text);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_resend_auto_policy_request(text,text,text,text,bigint,uuid) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_operator_jwt_resend_auto_policy_audit_read(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_after uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_result jsonb;
BEGIN
 PERFORM public.novu_rust_operator_jwt_resend_auto_policy_access(p_issuer,p_subject,p_client,p_env,p_exp,'audit');
 WITH candidates AS MATERIALIZED (SELECT * FROM public.novu_rust_operator_jwt_resend_auto_policy_audit WHERE environment_id=p_env AND (p_after IS NULL OR id>p_after) ORDER BY id LIMIT 51),
 page AS MATERIALIZED (SELECT * FROM candidates ORDER BY id LIMIT 50)
 SELECT jsonb_build_object('data',coalesce((SELECT jsonb_agg(jsonb_build_object('id',id,'actorIssuer',actor_issuer,'actorSubject',actor_subject,'actorClientId',actor_client,'databasePrincipal',database_principal,'requestId',request_id,'previous',previous_policy,'current',current_policy,'reason',reason,'recordedAt',recorded_at::text) ORDER BY id) FROM page),'[]'::jsonb),'hasMore',(SELECT count(*)>50 FROM candidates),'next',CASE WHEN (SELECT count(*)>50 FROM candidates) THEN (SELECT id::text FROM page ORDER BY id DESC LIMIT 1) ELSE NULL END) INTO v_result;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN v_result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_resend_auto_policy_audit_read(text,text,text,text,bigint,uuid) FROM PUBLIC;
