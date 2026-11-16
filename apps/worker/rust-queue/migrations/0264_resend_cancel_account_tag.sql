-- Operator-provided label, not an authenticated Resend account identifier.
ALTER TABLE public.novu_rust_resend_attempts ADD COLUMN account_tag text CHECK(account_tag IS NULL OR (octet_length(account_tag)<=128 AND account_tag ~ '^[A-Za-z0-9._-]+$'));
CREATE FUNCTION public.novu_rust_resend_cancel_reserve_tagged(p_env text,p_request uuid,p_job uuid,p_reason text,p_tag text) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_tag IS NULL OR octet_length(p_tag) NOT BETWEEN 1 AND 128 OR p_tag !~ '^[A-Za-z0-9._-]+$' THEN RAISE EXCEPTION 'invalid account tag' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_resend_cancel_access(p_env,true);
 PERFORM 1 FROM public.novu_queue_jobs WHERE environment_id=p_env AND id=p_job FOR UPDATE;
 IF NOT EXISTS(SELECT 1 FROM public.novu_rust_resend_attempts WHERE environment_id=p_env AND job_id=p_job AND account_tag=p_tag) THEN RAISE EXCEPTION 'account tag mismatch' USING ERRCODE='55000'; END IF;
 RETURN public.novu_rust_resend_cancel_reserve(p_env,p_request,p_job,p_reason);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_cancel_reserve_tagged(text,uuid,uuid,text,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_resend_cancel_claim_tagged(p_env text,p_tag text) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_cmd public.novu_rust_resend_cancel_commands%ROWTYPE;v_result jsonb;
BEGIN
 IF p_tag IS NULL OR octet_length(p_tag) NOT BETWEEN 1 AND 128 OR p_tag !~ '^[A-Za-z0-9._-]+$' THEN RAISE EXCEPTION 'invalid account tag' USING ERRCODE='22023'; END IF;
 PERFORM 1 FROM public.novu_rust_resend_cancel_command_workers g JOIN pg_catalog.pg_roles r ON r.oid=g.principal_oid AND r.rolname=g.database_principal AND r.rolcanlogin WHERE g.environment_id=p_env AND g.database_principal=session_user FOR SHARE OF g;
 IF NOT FOUND THEN RAISE EXCEPTION 'cancellation worker denied' USING ERRCODE='42501'; END IF;
 SELECT * INTO v_cmd FROM public.novu_rust_resend_cancel_commands WHERE environment_id=p_env AND state='pending' ORDER BY created_at,request_id FOR UPDATE SKIP LOCKED LIMIT 1;
 IF NOT FOUND THEN RETURN NULL; END IF;
 BEGIN
  v_result:=public.novu_rust_resend_cancel_reserve_tagged(p_env,v_cmd.request_id,v_cmd.job_id,v_cmd.reason,p_tag);
  IF NOT (v_result->>'fresh')::boolean THEN RAISE EXCEPTION 'already reserved' USING ERRCODE='55000'; END IF;
 EXCEPTION WHEN SQLSTATE '55000' OR SQLSTATE '23505' THEN
  UPDATE public.novu_rust_resend_cancel_commands SET state='failed',updated_at=clock_timestamp() WHERE environment_id=p_env AND request_id=v_cmd.request_id;
  RETURN jsonb_build_object('requestId',v_cmd.request_id,'state','failed');
 END;
 UPDATE public.novu_rust_resend_cancel_commands SET state='dispatching',updated_at=clock_timestamp() WHERE environment_id=p_env AND request_id=v_cmd.request_id;
 RETURN jsonb_build_object('requestId',v_cmd.request_id,'state','dispatching','providerId',v_result->>'providerId');
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_cancel_claim_tagged(text,text) FROM PUBLIC;
