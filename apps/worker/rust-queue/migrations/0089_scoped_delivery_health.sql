-- Aggregate-only monitoring functions for environment-scoped alert readers.
CREATE OR REPLACE FUNCTION public.novu_rust_assert_alert_view(p_environment text) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  IF p_environment IS NULL OR length(btrim(p_environment))=0 THEN RAISE EXCEPTION 'environment required' USING ERRCODE='22023'; END IF;
  IF NOT public.novu_rust_lock_operator_grant(p_environment,'alert_view') THEN RAISE EXCEPTION 'alert view permission denied' USING ERRCODE='42501'; END IF;
  IF NOT EXISTS(SELECT 1 FROM public.novu_rust_environments e WHERE e.id=p_environment) THEN RAISE EXCEPTION 'environment not found' USING ERRCODE='22023'; END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_assert_alert_view(text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_delivery_health(p_environment text)
RETURNS TABLE(unknown_count bigint,stranded_count bigint,oldest_unknown text,oldest_stranded text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  PERFORM public.novu_rust_assert_alert_view(p_environment);
  RETURN QUERY SELECT count(*) FILTER(WHERE a.state='unknown'),count(*) FILTER(WHERE a.state='dispatching' AND (j.status<>'running' OR j.lease_until<=statement_timestamp())),
    min(a.updated_at) FILTER(WHERE a.state='unknown')::text,min(a.updated_at) FILTER(WHERE a.state='dispatching' AND (j.status<>'running' OR j.lease_until<=statement_timestamp()))::text
    FROM public.novu_queue_jobs j JOIN (
      SELECT job_id,environment_id,state,updated_at FROM public.novu_rust_sendgrid_attempts
      UNION ALL SELECT job_id,environment_id,state,updated_at FROM public.novu_rust_resend_attempts
      UNION ALL SELECT job_id,environment_id,state,updated_at FROM public.novu_rust_twilio_attempts
      UNION ALL SELECT job_id,environment_id,state,updated_at FROM public.novu_rust_fcm_attempts
    ) a ON a.job_id=j.id AND a.environment_id=j.environment_id WHERE j.environment_id=p_environment;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_delivery_health(text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_queue_health(p_environment text,p_wait integer)
RETURNS TABLE(overdue bigint,expired bigint,oldest_overdue text,oldest_expired text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  IF p_wait IS NULL OR p_wait NOT BETWEEN 1 AND 86400 THEN RAISE EXCEPTION 'invalid wait threshold' USING ERRCODE='22023'; END IF;
  PERFORM public.novu_rust_assert_alert_view(p_environment);
  RETURN QUERY SELECT count(*) FILTER(WHERE j.status='pending' AND j.available_at<=statement_timestamp()-p_wait*interval '1 second'),count(*) FILTER(WHERE j.status='running' AND j.lease_until<=statement_timestamp()),
    min(j.available_at) FILTER(WHERE j.status='pending' AND j.available_at<=statement_timestamp()-p_wait*interval '1 second')::text,min(j.lease_until) FILTER(WHERE j.status='running' AND j.lease_until<=statement_timestamp())::text
    FROM public.novu_queue_jobs j WHERE j.environment_id=p_environment;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_queue_health(text,integer) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_provider_health(p_environment text,p_provider text,p_grace integer,p_heartbeat integer)
RETURNS TABLE(overdue bigint,eligible bigint,last_projection text,stale boolean)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  IF p_provider IS NULL OR p_provider NOT IN ('resend','twilio') OR p_grace IS NULL OR p_grace NOT BETWEEN 1 AND 86400 OR p_heartbeat IS NULL OR p_heartbeat NOT BETWEEN 1 AND 86400 THEN RAISE EXCEPTION 'invalid provider or threshold' USING ERRCODE='22023'; END IF;
  PERFORM public.novu_rust_assert_alert_view(p_environment);
  IF p_provider='resend' THEN
    RETURN QUERY SELECT count(*) FILTER(WHERE j.resend_delivery_result IS DISTINCT FROM 'delivered'),count(*),
      (SELECT h.last_completed_at::text FROM public.novu_rust_resend_projection_heartbeat h WHERE h.environment_id=p_environment),
      coalesce((SELECT h.last_completed_at<statement_timestamp()-p_heartbeat*interval '1 second' FROM public.novu_rust_resend_projection_heartbeat h WHERE h.environment_id=p_environment),true)
      FROM public.novu_queue_jobs j WHERE j.environment_id=p_environment AND j.kind='experimental_resend_email' AND j.status<>'cancelled' AND j.available_at<=statement_timestamp()-p_grace*interval '1 second';
  ELSE
    RETURN QUERY SELECT count(*) FILTER(WHERE j.twilio_delivery_result IS DISTINCT FROM 'delivered'),count(*),
      (SELECT h.last_completed_at::text FROM public.novu_rust_twilio_projection_heartbeat h WHERE h.environment_id=p_environment),
      coalesce((SELECT h.last_completed_at<statement_timestamp()-p_heartbeat*interval '1 second' FROM public.novu_rust_twilio_projection_heartbeat h WHERE h.environment_id=p_environment),true)
      FROM public.novu_queue_jobs j WHERE j.environment_id=p_environment AND j.kind='experimental_twilio_sms' AND j.status<>'cancelled' AND j.available_at<=statement_timestamp()-p_grace*interval '1 second';
  END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_provider_health(text,text,integer,integer) FROM PUBLIC;
