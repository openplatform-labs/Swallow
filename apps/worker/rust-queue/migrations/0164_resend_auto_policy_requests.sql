-- Request records and policy changes commit together; no secrets belong in reason.
ALTER TABLE public.novu_rust_resend_auto_policy_audit ADD COLUMN IF NOT EXISTS request_id uuid;
CREATE TABLE public.novu_rust_resend_auto_policy_requests (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),
 database_principal text NOT NULL,
 principal_oid oid NOT NULL,
 request_id uuid NOT NULL,
 expected_revision bigint NOT NULL,
 enabled boolean NOT NULL,
 max_retries integer NOT NULL,
 cutoff text NOT NULL,
 reason text NOT NULL,
 result jsonb,
 recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(environment_id,database_principal,principal_oid,request_id)
);
REVOKE ALL ON public.novu_rust_resend_auto_policy_requests FROM PUBLIC;

CREATE FUNCTION public.novu_rust_resend_auto_policy_set_v2(
 p_environment text,p_request uuid,p_expected bigint,p_enabled boolean,p_max integer,p_cutoff text,p_reason text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_oid oid; v_inserted boolean; v_request public.novu_rust_resend_auto_policy_requests%ROWTYPE; v_result jsonb; v_audit bigint;
BEGIN
 IF p_request IS NULL OR p_expected IS NULL OR p_expected < -1 OR p_enabled IS NULL OR p_max IS NULL OR p_max NOT BETWEEN 1 AND 10
  OR p_cutoff IS NULL OR p_cutoff NOT IN ('now','preserve') OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN
  RAISE EXCEPTION 'invalid policy request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_resend_auto_policy_access(p_environment,'write');
 SELECT oid INTO v_oid FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin;
 INSERT INTO public.novu_rust_resend_auto_policy_requests(environment_id,database_principal,principal_oid,request_id,expected_revision,enabled,max_retries,cutoff,reason)
 VALUES(p_environment,session_user,v_oid,p_request,p_expected,p_enabled,p_max,p_cutoff,p_reason)
 ON CONFLICT DO NOTHING RETURNING true INTO v_inserted;
 IF NOT coalesce(v_inserted,false) THEN
  SELECT * INTO v_request FROM public.novu_rust_resend_auto_policy_requests
   WHERE environment_id=p_environment AND database_principal=session_user AND principal_oid=v_oid AND request_id=p_request;
  IF NOT FOUND OR v_request.result IS NULL THEN RAISE EXCEPTION 'request unavailable' USING ERRCODE='55000'; END IF;
  IF v_request.expected_revision IS DISTINCT FROM p_expected OR v_request.enabled IS DISTINCT FROM p_enabled
   OR v_request.max_retries IS DISTINCT FROM p_max OR v_request.cutoff IS DISTINCT FROM p_cutoff OR v_request.reason IS DISTINCT FROM p_reason THEN
   RAISE EXCEPTION 'request ID reused with different content' USING ERRCODE='23505'; END IF;
  RETURN jsonb_build_object('requestId',p_request,'replayed',true,'policy',v_request.result);
 END IF;
 v_result := public.novu_rust_resend_auto_policy_set(p_environment,p_expected,p_enabled,p_max,p_cutoff,p_reason);
 SELECT id INTO v_audit FROM public.novu_rust_resend_auto_policy_audit
  WHERE environment_id=p_environment AND database_principal=session_user ORDER BY id DESC LIMIT 1;
 IF v_audit IS NULL THEN RAISE EXCEPTION 'missing policy audit' USING ERRCODE='55000'; END IF;
 UPDATE public.novu_rust_resend_auto_policy_audit SET request_id=p_request WHERE id=v_audit;
 UPDATE public.novu_rust_resend_auto_policy_requests SET result=v_result
  WHERE environment_id=p_environment AND database_principal=session_user AND principal_oid=v_oid AND request_id=p_request;
 RETURN jsonb_build_object('requestId',p_request,'replayed',false,'policy',v_result);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_auto_policy_set_v2(text,uuid,bigint,boolean,integer,text,text) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_resend_auto_policy_request(p_environment text,p_request uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_row public.novu_rust_resend_auto_policy_requests%ROWTYPE;
BEGIN
 IF p_request IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_resend_auto_policy_access(p_environment,'write');
 SELECT * INTO v_row FROM public.novu_rust_resend_auto_policy_requests
  WHERE environment_id=p_environment AND database_principal=session_user
   AND principal_oid=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin) AND request_id=p_request;
 IF NOT FOUND THEN RETURN jsonb_build_object('found',false); END IF;
 RETURN jsonb_build_object('found',true,'requestId',v_row.request_id,'expectedRevision',v_row.expected_revision::text,
  'enabled',v_row.enabled,'maxRetries',v_row.max_retries,'cutoff',v_row.cutoff,'reason',v_row.reason,'policy',v_row.result,'recordedAt',v_row.recorded_at::text);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_auto_policy_request(text,uuid) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_resend_auto_policy_audit_read(p_environment text,p_before bigint)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_result jsonb;
BEGIN
 IF p_before IS NOT NULL AND p_before<=0 THEN RAISE EXCEPTION 'invalid cursor' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_resend_auto_policy_access(p_environment,'audit');
 WITH candidates AS MATERIALIZED (SELECT * FROM public.novu_rust_resend_auto_policy_audit
  WHERE environment_id=p_environment AND (p_before IS NULL OR id<p_before) ORDER BY id DESC LIMIT 51),
 page AS MATERIALIZED (SELECT * FROM candidates ORDER BY id DESC LIMIT 50)
 SELECT jsonb_build_object('data',coalesce((SELECT jsonb_agg(jsonb_build_object('id',id::text,'requestId',request_id,'databasePrincipal',database_principal,'previous',previous_policy,'current',current_policy,'reason',reason,'recordedAt',recorded_at::text) ORDER BY id DESC) FROM page),'[]'::jsonb),
  'next',CASE WHEN (SELECT count(*) FROM candidates)>50 THEN (SELECT min(id)::text FROM page) ELSE NULL END) INTO v_result;
 RETURN v_result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_auto_policy_audit_read(text,bigint) FROM PUBLIC;
