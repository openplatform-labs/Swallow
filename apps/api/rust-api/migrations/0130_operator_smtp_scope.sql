-- JWT and database-login permissions are independently required.
CREATE TABLE public.novu_rust_operator_jwt_smtp_scope_grants (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,
 can_read boolean NOT NULL DEFAULT false,can_write boolean NOT NULL DEFAULT false,can_audit boolean NOT NULL DEFAULT false,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
CREATE TABLE public.novu_rust_operator_jwt_smtp_scope_requests (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,request_id uuid NOT NULL,
 target text NOT NULL,expected text NOT NULL,desired boolean NOT NULL,reason text NOT NULL,result jsonb NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(issuer,subject,client_id,environment_id,request_id)
);
CREATE TABLE public.novu_rust_operator_jwt_smtp_scope_audit (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),environment_id text NOT NULL,actor_issuer text NOT NULL,actor_subject text NOT NULL,actor_client text NOT NULL,
 database_principal name NOT NULL,request_id uuid NOT NULL,target text NOT NULL,expected text NOT NULL,desired boolean NOT NULL,result jsonb NOT NULL,reason text NOT NULL,recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX novu_rust_jwt_smtp_scope_audit_idx ON public.novu_rust_operator_jwt_smtp_scope_audit(environment_id,id);
REVOKE ALL ON public.novu_rust_operator_jwt_smtp_scope_grants,public.novu_rust_operator_jwt_smtp_scope_requests,public.novu_rust_operator_jwt_smtp_scope_audit FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_smtp_scope_access(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_access text) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_access IS NULL OR p_access NOT IN ('read','write','audit') THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_smtp_scope_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env AND CASE p_access WHEN 'read' THEN g.can_read WHEN 'write' THEN g.can_write ELSE g.can_audit END FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF p_access='audit' THEN
  PERFORM 1 FROM public.novu_rust_smtp_scope_audit_viewers v JOIN pg_catalog.pg_roles r ON r.oid=v.principal_oid AND r.rolname=v.database_principal AND r.rolcanlogin WHERE v.environment_id=p_env AND v.database_principal=session_user FOR SHARE OF v;
  IF NOT FOUND THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 ELSE PERFORM public.novu_rust_smtp_scope_admin_access(p_env,p_access='write'); END IF;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_smtp_scope_access(text,text,text,text,bigint,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_smtp_scope_read(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_target text) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb;
BEGIN
 PERFORM public.novu_rust_operator_jwt_smtp_scope_access(p_issuer,p_subject,p_client,p_env,p_exp,'read');
 result:=public.novu_rust_smtp_scope_admin_read(p_env,p_target);
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_smtp_scope_read(text,text,text,text,bigint,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_smtp_scope_change(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid,p_target text,p_expected text,p_desired boolean,p_reason text) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_smtp_scope_requests%ROWTYPE;v_result jsonb;v_worker_request uuid;
BEGIN
 IF p_request IS NULL OR p_target IS NULL OR octet_length(p_target) NOT BETWEEN 1 AND 63 OR p_expected IS NULL OR p_expected NOT IN ('absent','enabled','disabled') OR p_desired IS NULL OR p_reason IS NULL OR octet_length(btrim(p_reason)) NOT BETWEEN 1 AND 512 OR octet_length(p_reason)>512 THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_smtp_scope_access(p_issuer,p_subject,p_client,p_env,p_exp,'write');
 PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(pg_catalog.jsonb_build_array('jwt-smtp-scope',p_issuer,p_subject,p_client,p_env,p_request)::text,0));
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 SELECT * INTO r FROM public.novu_rust_operator_jwt_smtp_scope_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
 IF FOUND THEN
  IF (r.target,r.expected,r.desired,r.reason) IS DISTINCT FROM (p_target,p_expected,p_desired,p_reason) THEN RAISE EXCEPTION 'request conflict' USING ERRCODE='P4090'; END IF;
  IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
  RETURN jsonb_build_object('requestId',p_request,'replayed',true,'result',r.result);
 END IF;
 v_worker_request:=gen_random_uuid();
 BEGIN
  v_result:=public.novu_rust_smtp_scope_admin_set(p_env,v_worker_request,p_target,p_expected,p_desired,p_reason);
 EXCEPTION WHEN SQLSTATE '40001' OR SQLSTATE '23505' THEN RAISE EXCEPTION 'scope conflict' USING ERRCODE='P4090'; END;
 INSERT INTO public.novu_rust_operator_jwt_smtp_scope_audit(environment_id,actor_issuer,actor_subject,actor_client,database_principal,request_id,target,expected,desired,result,reason) VALUES(p_env,p_issuer,p_subject,p_client,session_user,p_request,p_target,p_expected,p_desired,v_result->'result',p_reason);
 INSERT INTO public.novu_rust_operator_jwt_smtp_scope_requests(issuer,subject,client_id,environment_id,request_id,target,expected,desired,reason,result) VALUES(p_issuer,p_subject,p_client,p_env,p_request,p_target,p_expected,p_desired,p_reason,v_result->'result');
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('requestId',p_request,'replayed',false,'result',v_result->'result');
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_smtp_scope_change(text,text,text,text,bigint,uuid,text,text,boolean,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_smtp_scope_request(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_smtp_scope_requests%ROWTYPE;
BEGIN
 IF p_request IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_smtp_scope_access(p_issuer,p_subject,p_client,p_env,p_exp,'write');
 SELECT * INTO r FROM public.novu_rust_operator_jwt_smtp_scope_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF NOT FOUND THEN RETURN jsonb_build_object('found',false); END IF;
 RETURN jsonb_build_object('found',true,'requestId',r.request_id,'target',r.target,'expected',r.expected,'desired',r.desired,'reason',r.reason,'result',r.result,'createdAt',r.created_at::text);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_smtp_scope_request(text,text,text,text,bigint,uuid) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_smtp_scope_audit_read(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_after uuid) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb;
BEGIN
 PERFORM public.novu_rust_operator_jwt_smtp_scope_access(p_issuer,p_subject,p_client,p_env,p_exp,'audit');
 WITH candidates AS MATERIALIZED (SELECT * FROM public.novu_rust_operator_jwt_smtp_scope_audit WHERE environment_id=p_env AND (p_after IS NULL OR id>p_after) ORDER BY id LIMIT 51),page AS MATERIALIZED (SELECT * FROM candidates ORDER BY id LIMIT 50)
 SELECT jsonb_build_object('data',coalesce((SELECT jsonb_agg(jsonb_build_object('id',id,'actorIssuer',actor_issuer,'actorSubject',actor_subject,'actorClientId',actor_client,'databasePrincipal',database_principal,'requestId',request_id,'target',target,'expected',expected,'desired',desired,'result',page.result,'reason',reason,'recordedAt',recorded_at::text) ORDER BY id) FROM page),'[]'::jsonb),'hasMore',(SELECT count(*)>50 FROM candidates),'next',CASE WHEN (SELECT count(*)>50 FROM candidates) THEN (SELECT id::text FROM page ORDER BY id DESC LIMIT 1) ELSE NULL END) INTO result;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_smtp_scope_audit_read(text,text,text,text,bigint,uuid) FROM PUBLIC;
