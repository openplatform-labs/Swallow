CREATE INDEX novu_rust_resend_cancel_unknown_id ON public.novu_rust_resend_cancel_requests(environment_id,request_id) WHERE state='unknown';
CREATE FUNCTION public.novu_rust_resend_cancel_list(p_env text,p_grace integer,p_after uuid)
RETURNS TABLE(request_id uuid,job_id uuid,created_at text,overdue boolean)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_grace IS NULL OR p_grace NOT BETWEEN 1 AND 86400 THEN RAISE EXCEPTION 'invalid grace' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_resend_cancel_access(p_env,false);
 RETURN QUERY SELECT r.request_id,r.job_id,r.created_at::text,r.created_at<=statement_timestamp()-p_grace*interval '1 second'
 FROM public.novu_rust_resend_cancel_requests r WHERE r.environment_id=p_env AND r.state='unknown' AND (p_after IS NULL OR r.request_id>p_after)
 ORDER BY r.request_id LIMIT 51;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_cancel_list(text,integer,uuid) FROM PUBLIC;
