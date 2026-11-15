CREATE INDEX novu_rust_resend_cancel_unknown_age ON public.novu_rust_resend_cancel_requests(environment_id,created_at) WHERE state='unknown';
CREATE FUNCTION public.novu_rust_resend_cancel_health(p_env text,p_grace integer) RETURNS TABLE(unknown bigint,overdue bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_grace IS NULL OR p_grace NOT BETWEEN 1 AND 86400 THEN RAISE EXCEPTION 'invalid grace' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_assert_alert_view(p_env);
 RETURN QUERY SELECT count(*),count(*) FILTER(WHERE r.created_at<=statement_timestamp()-p_grace*interval '1 second') FROM public.novu_rust_resend_cancel_requests r WHERE r.environment_id=p_env AND r.state='unknown';
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_cancel_health(text,integer) FROM PUBLIC;
