CREATE INDEX novu_rust_resend_cancel_legacy_ambiguous_idx ON public.novu_rust_resend_cancel_requests(environment_id,request_id) WHERE state='rejected' AND http_status IN (408,409);
CREATE FUNCTION public.novu_rust_resend_cancel_legacy_health(p_env text) RETURNS bigint
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE n bigint;
BEGIN
 PERFORM public.novu_rust_assert_alert_view(p_env);
 SELECT count(*) INTO n FROM public.novu_rust_resend_cancel_requests r WHERE r.environment_id=p_env AND r.state='rejected' AND r.http_status IN (408,409);
 RETURN n;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_cancel_legacy_health(text) FROM PUBLIC;
