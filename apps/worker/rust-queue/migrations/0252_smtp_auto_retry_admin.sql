-- Independent administrator login/OID and atomic, idempotent policy changes.
ALTER TABLE public.novu_rust_smtp_auto_retry_policy ADD COLUMN revision bigint NOT NULL DEFAULT 0 CHECK(revision>=0);
CREATE TABLE public.novu_rust_smtp_auto_admins (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),database_principal name NOT NULL,principal_oid oid NOT NULL,
 can_read boolean NOT NULL DEFAULT false,can_write boolean NOT NULL DEFAULT false,can_audit boolean NOT NULL DEFAULT false,
 PRIMARY KEY(environment_id,database_principal)
);
CREATE TABLE public.novu_rust_smtp_auto_policy_requests (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),database_principal name NOT NULL,principal_oid oid NOT NULL,request_id uuid NOT NULL,
 expected_revision bigint NOT NULL,enabled boolean NOT NULL,max_retries integer NOT NULL,delay_seconds integer NOT NULL,cutoff text NOT NULL,reason text NOT NULL,
 result jsonb,recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(environment_id,database_principal,principal_oid,request_id)
);
CREATE TABLE public.novu_rust_smtp_auto_policy_audit (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),database_principal name NOT NULL,principal_oid oid NOT NULL,
 request_id uuid NOT NULL,previous_policy jsonb NOT NULL,current_policy jsonb NOT NULL,cutoff text NOT NULL,reason text NOT NULL,recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX novu_rust_smtp_auto_policy_audit_page ON public.novu_rust_smtp_auto_policy_audit(environment_id,id DESC);
REVOKE ALL ON public.novu_rust_smtp_auto_admins,public.novu_rust_smtp_auto_policy_requests,public.novu_rust_smtp_auto_policy_audit FROM PUBLIC;
CREATE FUNCTION public.novu_rust_smtp_auto_admin_access(p_env text,p_access text) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_env IS NULL OR p_env='' OR p_access IS NULL OR p_access NOT IN ('read','write','audit') THEN RAISE EXCEPTION 'invalid SMTP auto policy request' USING ERRCODE='22023'; END IF;
 PERFORM 1 FROM public.novu_rust_smtp_auto_admins g JOIN pg_catalog.pg_roles r ON r.rolname=g.database_principal AND r.oid=g.principal_oid AND r.rolcanlogin
 WHERE g.environment_id=p_env AND g.database_principal=session_user AND CASE p_access WHEN 'read' THEN g.can_read WHEN 'write' THEN g.can_write ELSE g.can_audit END FOR SHARE OF g;
 IF NOT FOUND THEN RAISE EXCEPTION 'SMTP auto policy access denied' USING ERRCODE='42501'; END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_smtp_auto_admin_access(text,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_smtp_auto_policy_read(p_env text) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_result jsonb;
BEGIN
 PERFORM public.novu_rust_smtp_auto_admin_access(p_env,'read');
 SELECT jsonb_build_object('enabled',enabled,'maxRetries',max_retries,'delaySeconds',delay_seconds,'eligibleSince',eligible_since::text,'revision',revision::text) INTO v_result FROM public.novu_rust_smtp_auto_retry_policy WHERE environment_id=p_env;
 IF NOT FOUND THEN RAISE EXCEPTION 'SMTP auto policy missing' USING ERRCODE='55000'; END IF;RETURN v_result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_smtp_auto_policy_read(text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_smtp_auto_policy_set(p_env text,p_request uuid,p_expected bigint,p_enabled boolean,p_max integer,p_delay integer,p_cutoff text,p_reason text) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_oid oid; v_inserted boolean; v_req public.novu_rust_smtp_auto_policy_requests%ROWTYPE; v_old public.novu_rust_smtp_auto_retry_policy%ROWTYPE; v_new public.novu_rust_smtp_auto_retry_policy%ROWTYPE; v_previous jsonb; v_result jsonb;
BEGIN
 IF p_request IS NULL OR p_expected IS NULL OR p_expected<0 OR p_enabled IS NULL OR p_max IS NULL OR p_max NOT BETWEEN 1 AND 10 OR p_delay IS NULL OR p_delay NOT BETWEEN 10 AND 86400 OR p_cutoff IS NULL OR p_cutoff NOT IN ('now','preserve','all') OR p_reason IS NULL OR octet_length(p_reason)>512 OR length(btrim(p_reason))=0 THEN RAISE EXCEPTION 'invalid SMTP auto policy request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_smtp_auto_admin_access(p_env,'write');
 SELECT oid INTO v_oid FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin;
 INSERT INTO public.novu_rust_smtp_auto_policy_requests(environment_id,database_principal,principal_oid,request_id,expected_revision,enabled,max_retries,delay_seconds,cutoff,reason) VALUES(p_env,session_user,v_oid,p_request,p_expected,p_enabled,p_max,p_delay,p_cutoff,p_reason) ON CONFLICT DO NOTHING RETURNING true INTO v_inserted;
 IF NOT coalesce(v_inserted,false) THEN
  SELECT * INTO v_req FROM public.novu_rust_smtp_auto_policy_requests WHERE environment_id=p_env AND database_principal=session_user AND principal_oid=v_oid AND request_id=p_request;
  IF NOT FOUND OR v_req.result IS NULL THEN RAISE EXCEPTION 'request unavailable' USING ERRCODE='55000'; END IF;
  IF (v_req.expected_revision,v_req.enabled,v_req.max_retries,v_req.delay_seconds,v_req.cutoff,v_req.reason) IS DISTINCT FROM (p_expected,p_enabled,p_max,p_delay,p_cutoff,p_reason) THEN RAISE EXCEPTION 'request conflict' USING ERRCODE='23505'; END IF;
  RETURN jsonb_build_object('requestId',p_request,'replayed',true,'policy',v_req.result);
 END IF;
 SELECT * INTO v_old FROM public.novu_rust_smtp_auto_retry_policy WHERE environment_id=p_env FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'SMTP auto policy missing' USING ERRCODE='55000'; END IF;
 IF v_old.revision<>p_expected THEN RAISE EXCEPTION 'stale policy revision' USING ERRCODE='40001'; END IF;
 IF p_enabled AND p_cutoff='preserve' AND v_old.eligible_since IS NULL THEN RAISE EXCEPTION 'SMTP auto cutoff missing' USING ERRCODE='22023'; END IF;
 v_previous:=jsonb_build_object('enabled',v_old.enabled,'maxRetries',v_old.max_retries,'delaySeconds',v_old.delay_seconds,'eligibleSince',v_old.eligible_since::text,'revision',v_old.revision::text);
 UPDATE public.novu_rust_smtp_auto_retry_policy SET enabled=p_enabled,max_retries=p_max,delay_seconds=p_delay,eligible_since=CASE p_cutoff WHEN 'now' THEN clock_timestamp() WHEN 'all' THEN '-infinity'::timestamptz ELSE eligible_since END,revision=revision+1 WHERE environment_id=p_env RETURNING * INTO v_new;
 v_result:=jsonb_build_object('enabled',v_new.enabled,'maxRetries',v_new.max_retries,'delaySeconds',v_new.delay_seconds,'eligibleSince',v_new.eligible_since::text,'revision',v_new.revision::text);
 INSERT INTO public.novu_rust_smtp_auto_policy_audit(environment_id,database_principal,principal_oid,request_id,previous_policy,current_policy,cutoff,reason) VALUES(p_env,session_user,v_oid,p_request,v_previous,v_result,p_cutoff,p_reason);
 UPDATE public.novu_rust_smtp_auto_policy_requests SET result=v_result WHERE environment_id=p_env AND database_principal=session_user AND principal_oid=v_oid AND request_id=p_request;
 RETURN jsonb_build_object('requestId',p_request,'replayed',false,'policy',v_result);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_smtp_auto_policy_set(text,uuid,bigint,boolean,integer,integer,text,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_smtp_auto_policy_request(p_env text,p_request uuid) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_smtp_auto_policy_requests%ROWTYPE;
BEGIN
 IF p_request IS NULL THEN RAISE EXCEPTION 'invalid SMTP auto policy request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_smtp_auto_admin_access(p_env,'write');
 SELECT * INTO r FROM public.novu_rust_smtp_auto_policy_requests WHERE environment_id=p_env AND database_principal=session_user AND principal_oid=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin) AND request_id=p_request;
 IF NOT FOUND THEN RETURN jsonb_build_object('found',false); END IF;
 RETURN jsonb_build_object('found',true,'requestId',r.request_id,'expectedRevision',r.expected_revision::text,'enabled',r.enabled,'maxRetries',r.max_retries,'delaySeconds',r.delay_seconds,'cutoff',r.cutoff,'reason',r.reason,'policy',r.result,'recordedAt',r.recorded_at::text);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_smtp_auto_policy_request(text,uuid) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_smtp_auto_policy_audit(p_env text,p_before bigint) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_result jsonb;
BEGIN
 IF p_before IS NOT NULL AND p_before<=0 THEN RAISE EXCEPTION 'invalid cursor' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_smtp_auto_admin_access(p_env,'audit');
 WITH candidates AS MATERIALIZED (SELECT * FROM public.novu_rust_smtp_auto_policy_audit WHERE environment_id=p_env AND (p_before IS NULL OR id<p_before) ORDER BY id DESC LIMIT 51), page AS MATERIALIZED (SELECT * FROM candidates ORDER BY id DESC LIMIT 50)
 SELECT jsonb_build_object('data',coalesce((SELECT jsonb_agg(jsonb_build_object('id',id::text,'requestId',request_id,'databasePrincipal',database_principal,'previous',previous_policy,'current',current_policy,'cutoff',cutoff,'reason',reason,'recordedAt',recorded_at::text) ORDER BY id DESC) FROM page),'[]'::jsonb),'next',CASE WHEN (SELECT count(*) FROM candidates)>50 THEN (SELECT min(id)::text FROM page) ELSE NULL END) INTO v_result;
 RETURN v_result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_smtp_auto_policy_audit(text,bigint) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_smtp_auto_history(p_env text,p_before bigint) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_result jsonb;
BEGIN
 IF p_before IS NOT NULL AND p_before<=0 THEN RAISE EXCEPTION 'invalid cursor' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_smtp_auto_admin_access(p_env,'audit');
 WITH candidates AS MATERIALIZED (SELECT * FROM public.novu_rust_smtp_auto_retries WHERE environment_id=p_env AND (p_before IS NULL OR id<p_before) ORDER BY id DESC LIMIT 51), page AS MATERIALIZED (SELECT * FROM candidates ORDER BY id DESC LIMIT 50)
 SELECT jsonb_build_object('data',coalesce((SELECT jsonb_agg(jsonb_build_object('id',id::text,'jobId',job_id,'attemptRevision',attempt_revision,'failureRevision',failure_revision::text,'databasePrincipal',database_principal,'maxRetries',policy_max,'delaySeconds',policy_delay_seconds,'recordedAt',recorded_at::text) ORDER BY id DESC) FROM page),'[]'::jsonb),'next',CASE WHEN (SELECT count(*) FROM candidates)>50 THEN (SELECT min(id)::text FROM page) ELSE NULL END) INTO v_result;
 RETURN v_result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_smtp_auto_history(text,bigint) FROM PUBLIC;
