-- Idempotent retention requests use the same independently authorized DB login.
CREATE TABLE public.novu_rust_resend_cancel_cred_retention_requests (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),
 principal_oid oid NOT NULL, database_principal name NOT NULL, request_id uuid NOT NULL,
 older_than_seconds integer NOT NULL, batch_limit integer NOT NULL, reason text NOT NULL,
 deleted_count bigint, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(environment_id,principal_oid,request_id)
);
REVOKE ALL ON public.novu_rust_resend_cancel_cred_retention_requests FROM PUBLIC;

CREATE FUNCTION public.novu_rust_resend_cancel_cred_retention_request_access(p_env text)
RETURNS oid LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE actor oid;
BEGIN
 SELECT r.oid INTO actor FROM pg_catalog.pg_roles r JOIN public.novu_rust_resend_cancel_cred_retention_operators g
 ON g.database_principal=r.rolname AND g.principal_oid=r.oid
 WHERE r.rolname=session_user AND r.rolcanlogin AND g.environment_id=p_env FOR SHARE OF g;
 IF actor IS NULL THEN RAISE EXCEPTION 'retention request access denied' USING ERRCODE='42501'; END IF;
 RETURN actor;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_cancel_cred_retention_request_access(text) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_resend_cancel_cred_retention_submit(p_env text,p_request uuid,p_age integer,p_limit integer,p_reason text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE actor oid; r public.novu_rust_resend_cancel_cred_retention_requests%ROWTYPE; inserted boolean; previous_reason text; amount bigint;
BEGIN
 IF p_env IS NULL OR p_env='' OR p_request IS NULL OR p_age IS NULL OR p_age NOT BETWEEN 86401 AND 31536000 OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 1000 OR p_reason IS NULL OR btrim(p_reason)='' OR octet_length(p_reason)>500 OR p_reason ~ '[[:cntrl:]]' THEN
  RAISE EXCEPTION 'invalid retention request' USING ERRCODE='22023';
 END IF;
 actor:=public.novu_rust_resend_cancel_cred_retention_request_access(p_env);
 INSERT INTO public.novu_rust_resend_cancel_cred_retention_requests(environment_id,principal_oid,database_principal,request_id,older_than_seconds,batch_limit,reason)
 VALUES(p_env,actor,session_user,p_request,p_age,p_limit,p_reason) ON CONFLICT DO NOTHING;
 inserted:=FOUND;
 SELECT * INTO r FROM public.novu_rust_resend_cancel_cred_retention_requests WHERE environment_id=p_env AND principal_oid=actor AND request_id=p_request;
 IF r.database_principal IS DISTINCT FROM session_user::name OR r.older_than_seconds IS DISTINCT FROM p_age OR r.batch_limit IS DISTINCT FROM p_limit OR r.reason IS DISTINCT FROM p_reason THEN
  RAISE EXCEPTION 'retention request conflict' USING ERRCODE='22023';
 END IF;
 IF NOT inserted THEN
  IF r.deleted_count IS NULL THEN RAISE EXCEPTION 'retention request incomplete' USING ERRCODE='55000'; END IF;
  RETURN jsonb_build_object('requestId',p_request,'deleted',r.deleted_count,'replayed',true);
 END IF;
 previous_reason:=pg_catalog.current_setting('swallow.resend_cancel_cred_retention_reason',true);
 PERFORM pg_catalog.set_config('swallow.resend_cancel_cred_retention_reason',p_reason,true);
 amount:=public.novu_rust_prune_resend_cancel_credential_health(p_env,p_age,p_limit);
 PERFORM pg_catalog.set_config('swallow.resend_cancel_cred_retention_reason',COALESCE(previous_reason,''),true);
 UPDATE public.novu_rust_resend_cancel_cred_retention_requests SET deleted_count=amount WHERE environment_id=p_env AND principal_oid=actor AND request_id=p_request;
 RETURN jsonb_build_object('requestId',p_request,'deleted',amount,'replayed',false);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_cancel_cred_retention_submit(text,uuid,integer,integer,text) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_resend_cancel_cred_retention_request(p_env text,p_request uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE actor oid; r public.novu_rust_resend_cancel_cred_retention_requests%ROWTYPE;
BEGIN
 IF p_env IS NULL OR p_env='' OR p_request IS NULL THEN RAISE EXCEPTION 'invalid retention request' USING ERRCODE='22023'; END IF;
 actor:=public.novu_rust_resend_cancel_cred_retention_request_access(p_env);
 SELECT * INTO r FROM public.novu_rust_resend_cancel_cred_retention_requests WHERE environment_id=p_env AND principal_oid=actor AND request_id=p_request AND database_principal=session_user;
 IF NOT FOUND THEN RETURN jsonb_build_object('found',false); END IF;
 RETURN jsonb_build_object('found',true,'requestId',p_request,'olderThanSeconds',r.older_than_seconds,'batchLimit',r.batch_limit,'reason',r.reason,'deleted',r.deleted_count,'createdAt',r.created_at);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_cancel_cred_retention_request(text,uuid) FROM PUBLIC;
