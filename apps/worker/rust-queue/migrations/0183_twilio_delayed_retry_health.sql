-- Aggregate monitor for delayed SMS retry backlog and callback-blocked work.
CREATE INDEX novu_rust_twilio_delayed_health_idx ON public.novu_queue_jobs(environment_id,status,available_at,lease_until)
 WHERE kind='experimental_twilio_sms' AND twilio_delayed_retry;
CREATE FUNCTION public.novu_rust_twilio_delayed_retry_health(p_env text,p_grace integer)
RETURNS TABLE(pending_overdue bigint,expired_lease bigint,callback_blocked bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_grace IS NULL OR p_grace NOT BETWEEN 1 AND 86400 THEN RAISE EXCEPTION 'invalid grace' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_assert_alert_view(p_env);
 RETURN QUERY SELECT count(*) FILTER (WHERE j.status='pending' AND j.available_at<=statement_timestamp()-p_grace*interval '1 second'),
 count(*) FILTER (WHERE j.status='running' AND j.lease_until<=statement_timestamp()-p_grace*interval '1 second'),
 count(*) FILTER (WHERE j.status='failed' AND j.last_error='sms retry blocked by provider callback')
 FROM public.novu_queue_jobs j WHERE j.environment_id=p_env AND j.kind='experimental_twilio_sms' AND j.twilio_delayed_retry;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_twilio_delayed_retry_health(text,integer) FROM PUBLIC;
