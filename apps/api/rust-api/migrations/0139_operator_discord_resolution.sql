-- JWT decisions are separate from review and DB login permissions.
CREATE TABLE public.novu_rust_operator_jwt_discord_resolution_grants (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,
 can_accept boolean NOT NULL DEFAULT false,can_retry boolean NOT NULL DEFAULT false,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
CREATE TABLE public.novu_rust_operator_jwt_discord_resolution_requests (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,request_id uuid NOT NULL,
 job_id uuid NOT NULL,attempt_revision uuid,failure_revision bigint NOT NULL,decision text NOT NULL,reason text NOT NULL,recorded boolean,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(issuer,subject,client_id,environment_id,request_id)
);
REVOKE ALL ON public.novu_rust_operator_jwt_discord_resolution_grants,public.novu_rust_operator_jwt_discord_resolution_requests FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_discord_resolution_access(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_decision text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE permitted boolean;
BEGIN
 IF p_decision IS NULL OR p_decision NOT IN ('accepted','retry') THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 SELECT CASE WHEN p_decision='accepted' THEN g.can_accept ELSE g.can_retry END INTO permitted FROM public.novu_rust_operator_jwt_discord_resolution_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT coalesce(permitted,false) OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 PERFORM 1 FROM public.novu_rust_discord_resolvers g JOIN pg_catalog.pg_roles r ON r.oid=g.principal_oid AND r.rolname=g.database_principal AND r.rolcanlogin WHERE g.environment_id=p_env AND g.database_principal=session_user AND CASE WHEN p_decision='accepted' THEN g.can_accept ELSE g.can_retry END FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_discord_resolution_access(text,text,text,text,bigint,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_discord_resolve(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid,p_job uuid,p_attempt uuid,p_failure bigint,p_decision text,p_reason text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE n integer; r public.novu_rust_operator_jwt_discord_resolution_requests%ROWTYPE; v_recorded boolean; v_db_request uuid;
BEGIN
 IF p_request IS NULL OR p_job IS NULL OR (p_attempt IS NULL AND p_decision IS DISTINCT FROM 'retry') OR p_failure IS NULL OR p_failure<0 OR p_reason IS NULL OR octet_length(btrim(p_reason)) NOT BETWEEN 1 AND 512 THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_discord_resolution_access(p_issuer,p_subject,p_client,p_env,p_exp,p_decision);
 INSERT INTO public.novu_rust_operator_jwt_discord_resolution_requests(issuer,subject,client_id,environment_id,request_id,job_id,attempt_revision,failure_revision,decision,reason) VALUES(p_issuer,p_subject,p_client,p_env,p_request,p_job,p_attempt,p_failure,p_decision,p_reason) ON CONFLICT DO NOTHING;
 GET DIAGNOSTICS n=ROW_COUNT;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF n=0 THEN
  SELECT * INTO r FROM public.novu_rust_operator_jwt_discord_resolution_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
  IF NOT FOUND OR (r.job_id,r.attempt_revision,r.failure_revision,r.decision,r.reason) IS DISTINCT FROM (p_job,p_attempt,p_failure,p_decision,p_reason) OR r.recorded IS NULL THEN RAISE EXCEPTION 'request id conflict' USING ERRCODE='P4090'; END IF;
  RETURN jsonb_build_object('recorded',r.recorded,'replayed',true);
 END IF;
 -- Isolate request IDs between JWT identities sharing a database login.
 v_db_request:=substr(encode(sha256(convert_to(jsonb_build_array('discord-jwt-resolution-v1',p_issuer,p_subject,p_client,p_env,p_request)::text,'UTF8')),'hex'),1,32)::uuid;
 BEGIN
  v_recorded:=public.novu_rust_resolve_discord(p_env,v_db_request,p_job,p_attempt,p_failure,p_decision,p_reason);
 EXCEPTION WHEN SQLSTATE '22023' THEN RAISE EXCEPTION 'resolution conflict' USING ERRCODE='P4090'; END;
 IF NOT v_recorded THEN RAISE EXCEPTION 'resolution request collision' USING ERRCODE='P4090'; END IF;
 UPDATE public.novu_rust_operator_jwt_discord_resolution_requests SET recorded=v_recorded WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('recorded',v_recorded,'replayed',false);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_discord_resolve(text,text,text,text,bigint,uuid,uuid,uuid,bigint,text,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_discord_resolution_request(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid,p_decision text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_discord_resolution_requests%ROWTYPE;
BEGIN
 IF p_request IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_discord_resolution_access(p_issuer,p_subject,p_client,p_env,p_exp,p_decision);
 SELECT * INTO r FROM public.novu_rust_operator_jwt_discord_resolution_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request AND decision=p_decision;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF NOT FOUND THEN RETURN jsonb_build_object('found',false); END IF;
 RETURN jsonb_build_object('found',true,'requestId',r.request_id,'jobId',r.job_id,'attemptRevision',r.attempt_revision,'failureRevision',r.failure_revision::text,'decision',r.decision,'reason',r.reason,'recorded',r.recorded,'createdAt',r.created_at::text);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_discord_resolution_request(text,text,text,text,bigint,uuid,text) FROM PUBLIC;

-- Add same-revision rate-limit guidance to the existing authorized attention page.
CREATE OR REPLACE FUNCTION public.novu_rust_operator_jwt_discord_attention(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_after uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE permitted boolean;result jsonb;
BEGIN
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 SELECT can_list INTO permitted FROM public.novu_rust_operator_jwt_discord_review_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT coalesce(permitted,false) OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 WITH candidates AS MATERIALIZED (SELECT * FROM public.novu_rust_discord_attention(p_env,p_after) ORDER BY job_id LIMIT 51),page AS MATERIALIZED (SELECT * FROM candidates ORDER BY job_id LIMIT 50)
 SELECT jsonb_build_object('data',coalesce((SELECT jsonb_agg(jsonb_build_object('jobId',p.job_id,'kind',p.kind,'status',p.status,'attemptState',p.attempt_state,'attemptRevision',p.attempt_revision,'failureRevision',p.failure_revision::text,'attempts',p.attempts,'httpStatus',p.http_status,'retryAfterSeconds',a.retry_after_seconds,'retryNotBefore',CASE WHEN a.http_status=429 AND a.retry_after_seconds IS NOT NULL THEN to_char((a.updated_at+make_interval(secs=>a.retry_after_seconds::double precision)) AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') ELSE NULL END) ORDER BY p.job_id) FROM page p LEFT JOIN public.novu_rust_discord_attempts a ON a.environment_id=p_env AND a.job_id=p.job_id AND a.revision=p.attempt_revision),'[]'::jsonb),'hasMore',(SELECT count(*)>50 FROM candidates),'next',CASE WHEN (SELECT count(*)>50 FROM candidates) THEN (SELECT job_id::text FROM page ORDER BY job_id DESC LIMIT 1) ELSE NULL END) INTO result;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_discord_attention(text,text,text,text,bigint,uuid) FROM PUBLIC;
