CREATE OR REPLACE FUNCTION public.novu_rust_sendgrid_health(p_environment text,p_grace integer,p_heartbeat integer)
RETURNS TABLE(overdue bigint,eligible bigint,last_projection text,stale boolean)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  IF p_grace IS NULL OR p_grace NOT BETWEEN 1 AND 86400 OR p_heartbeat IS NULL OR p_heartbeat NOT BETWEEN 1 AND 86400 THEN RAISE EXCEPTION 'invalid threshold' USING ERRCODE='22023'; END IF;
  PERFORM public.novu_rust_assert_alert_view(p_environment);
  RETURN QUERY SELECT count(*) FILTER(WHERE j.sendgrid_delivery_result IS DISTINCT FROM 'delivered'),count(*),
    (SELECT h.last_completed_at::text FROM public.novu_rust_sendgrid_projection_heartbeat h WHERE h.environment_id=p_environment),
    coalesce((SELECT h.last_completed_at<statement_timestamp()-p_heartbeat*interval '1 second' FROM public.novu_rust_sendgrid_projection_heartbeat h WHERE h.environment_id=p_environment),true)
    FROM public.novu_queue_jobs j WHERE j.environment_id=p_environment AND j.kind='experimental_sendgrid_email' AND j.status<>'cancelled' AND j.available_at<=statement_timestamp()-p_grace*interval '1 second';
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_sendgrid_health(text,integer,integer) FROM PUBLIC;
