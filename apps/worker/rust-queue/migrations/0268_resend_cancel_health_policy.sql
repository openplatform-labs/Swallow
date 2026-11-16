-- Optional scoped health policy: exclude only provider-confirmed, uncontradicted scheduled cancellations.
CREATE FUNCTION public.novu_rust_resend_health_exclude_confirmed_cancel(p_environment text,p_grace integer,p_heartbeat integer)
RETURNS TABLE(overdue bigint,eligible bigint,last_projection text,stale boolean)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_grace IS NULL OR p_grace NOT BETWEEN 1 AND 86400 OR p_heartbeat IS NULL OR p_heartbeat NOT BETWEEN 1 AND 86400 THEN RAISE EXCEPTION 'invalid threshold' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_assert_alert_view(p_environment);
    RETURN QUERY SELECT count(*) FILTER(WHERE (j.resend_delivery_result IS DISTINCT FROM 'delivered' OR EXISTS(SELECT 1 FROM public.novu_rust_resend_attempts a JOIN public.novu_rust_resend_attempts b ON b.environment_id=a.environment_id AND b.provider_message_id=a.provider_message_id AND b.job_id<>a.job_id WHERE a.environment_id=j.environment_id AND a.job_id=j.id AND a.provider_message_id IS NOT NULL AND a.provider_message_id<>''))),count(*),
      (SELECT h.last_completed_at::text FROM public.novu_rust_resend_projection_heartbeat h WHERE h.environment_id=p_environment),
      coalesce((SELECT h.last_completed_at<statement_timestamp()-p_heartbeat*interval '1 second' FROM public.novu_rust_resend_projection_heartbeat h WHERE h.environment_id=p_environment),true)
      FROM public.novu_queue_jobs j WHERE j.environment_id=p_environment AND j.kind='experimental_resend_email' AND j.status<>'cancelled'
      AND NOT (j.resend_scheduled_at IS NOT NULL AND (j.resend_delivery_result IS NULL OR j.resend_delivery_result='pending')
       AND EXISTS (SELECT 1 FROM public.novu_rust_resend_cancel_requests cr
        WHERE cr.environment_id=j.environment_id AND cr.job_id=j.id AND cr.state='confirmed'
         AND NOT EXISTS(SELECT 1 FROM public.novu_rust_resend_cancel_resolutions rr WHERE rr.environment_id=cr.environment_id AND rr.request_id=cr.request_id))
       AND public.novu_rust_channel_result(j.environment_id,j.id,'resend')->>'cancellationEvidence'='no_conflict_observed')
      AND (CASE WHEN j.resend_scheduled_at IS NOT NULL AND (j.resend_delivery_result IS NULL OR j.resend_delivery_result='pending') AND EXISTS(SELECT 1 FROM public.novu_rust_resend_attempts x WHERE x.job_id=j.id AND x.environment_id=j.environment_id AND x.state='accepted') AND NOT EXISTS(SELECT 1 FROM public.novu_rust_resend_attempts a JOIN public.novu_rust_resend_attempts b ON b.environment_id=a.environment_id AND b.provider_message_id=a.provider_message_id AND b.job_id<>a.job_id WHERE a.environment_id=j.environment_id AND a.job_id=j.id AND a.provider_message_id IS NOT NULL AND a.provider_message_id<>'') THEN greatest(j.available_at,j.resend_scheduled_at) ELSE j.available_at END)<=statement_timestamp()-p_grace*interval '1 second';
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_health_exclude_confirmed_cancel(text,integer,integer) FROM PUBLIC;
