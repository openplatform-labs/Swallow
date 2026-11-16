-- Timeout/conflict responses do not prove cancellation failed.
CREATE OR REPLACE FUNCTION public.novu_rust_resend_cancel_finish(p_env text,p_request uuid,p_state text,p_status integer) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_oid oid; v_row public.novu_rust_resend_cancel_requests%ROWTYPE;
BEGIN
 IF p_state IS NULL OR p_state NOT IN ('unknown','confirmed','rejected') OR (p_state='rejected' AND (p_status IS NULL OR p_status NOT BETWEEN 400 AND 499 OR p_status IN (408,409))) OR (p_state<>'rejected' AND p_status IS NOT NULL) THEN RAISE EXCEPTION 'invalid cancellation result' USING ERRCODE='22023'; END IF;
 v_oid:=public.novu_rust_resend_cancel_access(p_env,true);
 SELECT * INTO v_row FROM public.novu_rust_resend_cancel_requests WHERE environment_id=p_env AND request_id=p_request FOR UPDATE;
 IF NOT FOUND OR v_row.database_principal<>session_user OR v_row.principal_oid<>v_oid THEN RAISE EXCEPTION 'cancellation request unavailable' USING ERRCODE='42501'; END IF;
 IF EXISTS(SELECT 1 FROM public.novu_rust_resend_cancel_resolutions WHERE environment_id=p_env AND request_id=p_request) THEN RAISE EXCEPTION 'cancellation already resolved by operator' USING ERRCODE='55000'; END IF;
 IF v_row.state<>'unknown' THEN
  IF (v_row.state,v_row.http_status) IS DISTINCT FROM (p_state,p_status) THEN RAISE EXCEPTION 'cancellation result conflict' USING ERRCODE='23505'; END IF;
 ELSE
  UPDATE public.novu_rust_resend_cancel_requests SET state=p_state,http_status=p_status,updated_at=clock_timestamp() WHERE environment_id=p_env AND request_id=p_request;
 END IF;
 RETURN jsonb_build_object('requestId',p_request,'state',p_state,'httpStatus',p_status);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_cancel_finish(text,uuid,text,integer) FROM PUBLIC;

-- Previous versions could have stored ambiguous HTTP codes as rejected. Do not
-- rewrite history: a later cancellation may already exist for the same job.
CREATE FUNCTION public.novu_rust_resend_cancel_legacy_ambiguous(p_env text,p_after uuid)
RETURNS TABLE(request_id uuid,job_id uuid,http_status integer,created_at text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 PERFORM public.novu_rust_resend_cancel_access(p_env,false);
 RETURN QUERY SELECT r.request_id,r.job_id,r.http_status,r.created_at::text
 FROM public.novu_rust_resend_cancel_requests r
 WHERE r.environment_id=p_env AND r.state='rejected' AND r.http_status IN (408,409) AND (p_after IS NULL OR r.request_id>p_after)
 ORDER BY r.request_id LIMIT 51;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_cancel_legacy_ambiguous(text,uuid) FROM PUBLIC;
