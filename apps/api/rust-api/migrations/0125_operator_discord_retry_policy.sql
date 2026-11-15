-- JWT permissions and API DB login policy permissions are both required per destination.
CREATE TABLE public.novu_rust_operator_jwt_discord_retry_policy_grants (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,destination_id text NOT NULL,
 can_read boolean NOT NULL DEFAULT false,can_write boolean NOT NULL DEFAULT false,can_audit boolean NOT NULL DEFAULT false,
 PRIMARY KEY(issuer,subject,client_id,environment_id,destination_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
CREATE TABLE public.novu_rust_operator_jwt_discord_retry_policy_requests (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL REFERENCES public.novu_rust_environments(id) ON DELETE CASCADE,destination_id text NOT NULL,request_id uuid NOT NULL,
 expected_revision bigint NOT NULL,mode text NOT NULL,max_retries integer NOT NULL,fallback_seconds integer NOT NULL,reason text NOT NULL,
 policy jsonb NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(issuer,subject,client_id,environment_id,destination_id,request_id)
);
CREATE TABLE public.novu_rust_operator_jwt_discord_retry_policy_audit (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),environment_id text NOT NULL,destination_id text NOT NULL,actor_issuer text NOT NULL,actor_subject text NOT NULL,actor_client text NOT NULL,
 database_principal name NOT NULL,request_id uuid NOT NULL,previous_policy jsonb,current_policy jsonb NOT NULL,reason text NOT NULL,recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX novu_rust_jwt_discord_retry_policy_audit_idx ON public.novu_rust_operator_jwt_discord_retry_policy_audit(environment_id,destination_id,id);
REVOKE ALL ON public.novu_rust_operator_jwt_discord_retry_policy_grants,public.novu_rust_operator_jwt_discord_retry_policy_requests,public.novu_rust_operator_jwt_discord_retry_policy_audit FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_discord_retry_policy_access(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_dest text,p_access text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_access IS NULL OR p_access NOT IN ('read','write','audit') THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_discord_retry_policy_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env AND g.destination_id=p_dest
  AND CASE p_access WHEN 'read' THEN g.can_read WHEN 'write' THEN g.can_write ELSE g.can_audit END FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 PERFORM public.novu_rust_discord_retry_policy_access(p_env,p_dest,p_access);
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_discord_retry_policy_access(text,text,text,text,bigint,text,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_discord_retry_policy_read(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_dest text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_result jsonb;
BEGIN
 PERFORM public.novu_rust_operator_jwt_discord_retry_policy_access(p_issuer,p_subject,p_client,p_env,p_exp,p_dest,'read');
 v_result:=public.novu_rust_discord_retry_policy_read(p_env,p_dest);
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN v_result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_discord_retry_policy_read(text,text,text,text,bigint,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_discord_retry_policy_change(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_dest text,p_request uuid,p_expected bigint,p_mode text,p_max integer,p_fallback integer,p_reason text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_discord_retry_policy_requests%ROWTYPE;v_worker_request uuid;v_result jsonb;v_previous jsonb;
BEGIN
 IF p_request IS NULL OR p_expected IS NULL OR p_expected<0 OR p_mode IS NULL OR p_mode NOT IN ('off','known','fallback') OR p_max IS NULL OR p_max NOT BETWEEN 0 AND 10 OR p_fallback IS NULL OR p_fallback NOT BETWEEN 1 AND 86400 OR p_reason IS NULL OR octet_length(btrim(p_reason)) NOT BETWEEN 1 AND 512 OR octet_length(p_reason)>512 THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_discord_retry_policy_access(p_issuer,p_subject,p_client,p_env,p_exp,p_dest,'write');
 PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(pg_catalog.jsonb_build_array('jwt-discord-retry-policy',p_issuer,p_subject,p_client,p_env,p_dest,p_request)::text,0));
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 SELECT * INTO r FROM public.novu_rust_operator_jwt_discord_retry_policy_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND destination_id=p_dest AND request_id=p_request;
 IF FOUND THEN
  IF (r.expected_revision,r.mode,r.max_retries,r.fallback_seconds,r.reason) IS DISTINCT FROM (p_expected,p_mode,p_max,p_fallback,p_reason) THEN RAISE EXCEPTION 'request conflict' USING ERRCODE='P4090'; END IF;
  IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
  RETURN jsonb_build_object('requestId',p_request,'replayed',true,'policy',r.policy);
 END IF;
 v_worker_request:=gen_random_uuid();
 BEGIN
  v_result:=public.novu_rust_discord_retry_policy_set(p_env,p_dest,v_worker_request,p_expected,p_mode,p_max,p_fallback,p_reason);
 EXCEPTION WHEN SQLSTATE '40001' THEN RAISE EXCEPTION 'stale policy revision' USING ERRCODE='P4090'; END;
 SELECT previous_policy INTO v_previous FROM public.novu_rust_discord_retry_policy_audit WHERE environment_id=p_env AND destination_id=p_dest AND database_principal=session_user AND request_id=v_worker_request;
 IF NOT FOUND THEN RAISE EXCEPTION 'missing policy audit' USING ERRCODE='55000'; END IF;
 INSERT INTO public.novu_rust_operator_jwt_discord_retry_policy_audit(environment_id,destination_id,actor_issuer,actor_subject,actor_client,database_principal,request_id,previous_policy,current_policy,reason) VALUES(p_env,p_dest,p_issuer,p_subject,p_client,session_user,p_request,v_previous,v_result->'policy',p_reason);
 INSERT INTO public.novu_rust_operator_jwt_discord_retry_policy_requests(issuer,subject,client_id,environment_id,destination_id,request_id,expected_revision,mode,max_retries,fallback_seconds,reason,policy) VALUES(p_issuer,p_subject,p_client,p_env,p_dest,p_request,p_expected,p_mode,p_max,p_fallback,p_reason,v_result->'policy');
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('requestId',p_request,'replayed',false,'policy',v_result->'policy');
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_discord_retry_policy_change(text,text,text,text,bigint,text,uuid,bigint,text,integer,integer,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_discord_retry_policy_request(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_dest text,p_request uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_discord_retry_policy_requests%ROWTYPE;
BEGIN
 IF p_request IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_discord_retry_policy_access(p_issuer,p_subject,p_client,p_env,p_exp,p_dest,'write');
 SELECT * INTO r FROM public.novu_rust_operator_jwt_discord_retry_policy_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND destination_id=p_dest AND request_id=p_request;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF NOT FOUND THEN RETURN jsonb_build_object('found',false); END IF;
 RETURN jsonb_build_object('found',true,'requestId',r.request_id,'expectedRevision',r.expected_revision::text,'mode',r.mode,'maxRetries',r.max_retries,'fallbackSeconds',r.fallback_seconds,'reason',r.reason,'policy',r.policy,'createdAt',r.created_at::text);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_discord_retry_policy_request(text,text,text,text,bigint,text,uuid) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_discord_retry_policy_audit_read(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_dest text,p_after uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_result jsonb;
BEGIN
 PERFORM public.novu_rust_operator_jwt_discord_retry_policy_access(p_issuer,p_subject,p_client,p_env,p_exp,p_dest,'audit');
 WITH candidates AS MATERIALIZED (SELECT * FROM public.novu_rust_operator_jwt_discord_retry_policy_audit WHERE environment_id=p_env AND destination_id=p_dest AND (p_after IS NULL OR id>p_after) ORDER BY id LIMIT 51),page AS MATERIALIZED (SELECT * FROM candidates ORDER BY id LIMIT 50)
 SELECT jsonb_build_object('data',coalesce((SELECT jsonb_agg(jsonb_build_object('id',id,'actorIssuer',actor_issuer,'actorSubject',actor_subject,'actorClientId',actor_client,'databasePrincipal',database_principal,'requestId',request_id,'previous',previous_policy,'current',current_policy,'reason',reason,'recordedAt',recorded_at::text) ORDER BY id) FROM page),'[]'::jsonb),'hasMore',(SELECT count(*)>50 FROM candidates),'next',CASE WHEN (SELECT count(*)>50 FROM candidates) THEN (SELECT id::text FROM page ORDER BY id DESC LIMIT 1) ELSE NULL END) INTO v_result;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN v_result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_discord_retry_policy_audit_read(text,text,text,text,bigint,text,uuid) FROM PUBLIC;
