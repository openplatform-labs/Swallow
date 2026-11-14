-- Detect duplicate provider IDs without waiting for the next projection scan.
CREATE OR REPLACE FUNCTION public.novu_rust_provider_health(p_environment text,p_provider text,p_grace integer,p_heartbeat integer)
RETURNS TABLE(overdue bigint,eligible bigint,last_projection text,stale boolean)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  IF p_provider IS NULL OR p_provider NOT IN ('resend','twilio') OR p_grace IS NULL OR p_grace NOT BETWEEN 1 AND 86400 OR p_heartbeat IS NULL OR p_heartbeat NOT BETWEEN 1 AND 86400 THEN RAISE EXCEPTION 'invalid provider or threshold' USING ERRCODE='22023'; END IF;
  PERFORM public.novu_rust_assert_alert_view(p_environment);
  IF p_provider='resend' THEN
    RETURN QUERY SELECT count(*) FILTER(WHERE (j.resend_delivery_result IS DISTINCT FROM 'delivered' OR EXISTS(SELECT 1 FROM public.novu_rust_resend_attempts a JOIN public.novu_rust_resend_attempts b ON b.environment_id=a.environment_id AND b.provider_message_id=a.provider_message_id AND b.job_id<>a.job_id WHERE a.environment_id=j.environment_id AND a.job_id=j.id AND a.provider_message_id IS NOT NULL AND a.provider_message_id<>''))),count(*),
      (SELECT h.last_completed_at::text FROM public.novu_rust_resend_projection_heartbeat h WHERE h.environment_id=p_environment),
      coalesce((SELECT h.last_completed_at<statement_timestamp()-p_heartbeat*interval '1 second' FROM public.novu_rust_resend_projection_heartbeat h WHERE h.environment_id=p_environment),true)
      FROM public.novu_queue_jobs j WHERE j.environment_id=p_environment AND j.kind='experimental_resend_email' AND j.status<>'cancelled' AND j.available_at<=statement_timestamp()-p_grace*interval '1 second';
  ELSE
    RETURN QUERY SELECT count(*) FILTER(WHERE (j.twilio_delivery_result IS DISTINCT FROM 'delivered' OR EXISTS(SELECT 1 FROM public.novu_rust_twilio_attempts a JOIN public.novu_rust_twilio_attempts b ON b.environment_id=a.environment_id AND b.provider_message_id=a.provider_message_id AND b.job_id<>a.job_id WHERE a.environment_id=j.environment_id AND a.job_id=j.id AND a.provider_message_id IS NOT NULL AND a.provider_message_id<>''))),count(*),
      (SELECT h.last_completed_at::text FROM public.novu_rust_twilio_projection_heartbeat h WHERE h.environment_id=p_environment),
      coalesce((SELECT h.last_completed_at<statement_timestamp()-p_heartbeat*interval '1 second' FROM public.novu_rust_twilio_projection_heartbeat h WHERE h.environment_id=p_environment),true)
      FROM public.novu_queue_jobs j WHERE j.environment_id=p_environment AND j.kind='experimental_twilio_sms' AND j.status<>'cancelled' AND j.available_at<=statement_timestamp()-p_grace*interval '1 second';
  END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_provider_health(text,text,integer,integer) FROM PUBLIC;

-- Stored per-job results without direct access to provider identifiers or payloads.
CREATE OR REPLACE FUNCTION public.novu_rust_channel_result(p_environment text,p_job uuid,p_provider text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb;
BEGIN
  PERFORM public.novu_rust_assert_alert_view(p_environment);
  IF p_job IS NULL OR p_provider IS NULL OR p_provider NOT IN ('resend','twilio') THEN
    RAISE EXCEPTION 'invalid job or provider' USING ERRCODE='22023';
  END IF;
  IF p_provider='resend' THEN
    SELECT jsonb_build_object('jobId',j.id,'jobStatus',j.status,'result',CASE WHEN EXISTS(SELECT 1 FROM public.novu_rust_resend_attempts a JOIN public.novu_rust_resend_attempts b ON b.environment_id=a.environment_id AND b.provider_message_id=a.provider_message_id AND b.job_id<>a.job_id WHERE a.environment_id=j.environment_id AND a.job_id=j.id AND a.provider_message_id IS NOT NULL AND a.provider_message_id<>'') THEN 'unknown' ELSE coalesce(j.resend_delivery_result,'unknown') END,'providerIdAmbiguous',EXISTS(SELECT 1 FROM public.novu_rust_resend_attempts a JOIN public.novu_rust_resend_attempts b ON b.environment_id=a.environment_id AND b.provider_message_id=a.provider_message_id AND b.job_id<>a.job_id WHERE a.environment_id=j.environment_id AND a.job_id=j.id AND a.provider_message_id IS NOT NULL AND a.provider_message_id<>''),
      'attemptState',a.state,'providerIdKnown',coalesce(length(a.provider_message_id)>0,false),
      'lastCompletedScanAt',(SELECT h.last_completed_at::text FROM public.novu_rust_resend_projection_heartbeat h WHERE h.environment_id=p_environment))
    INTO result FROM public.novu_queue_jobs j LEFT JOIN public.novu_rust_resend_attempts a ON a.environment_id=j.environment_id AND a.job_id=j.id
    WHERE j.id=p_job AND j.environment_id=p_environment AND j.kind='experimental_resend_email';
  ELSE
    SELECT jsonb_build_object('jobId',j.id,'jobStatus',j.status,'result',CASE WHEN EXISTS(SELECT 1 FROM public.novu_rust_twilio_attempts a JOIN public.novu_rust_twilio_attempts b ON b.environment_id=a.environment_id AND b.provider_message_id=a.provider_message_id AND b.job_id<>a.job_id WHERE a.environment_id=j.environment_id AND a.job_id=j.id AND a.provider_message_id IS NOT NULL AND a.provider_message_id<>'') THEN 'unknown' ELSE coalesce(j.twilio_delivery_result,'unknown') END,'providerIdAmbiguous',EXISTS(SELECT 1 FROM public.novu_rust_twilio_attempts a JOIN public.novu_rust_twilio_attempts b ON b.environment_id=a.environment_id AND b.provider_message_id=a.provider_message_id AND b.job_id<>a.job_id WHERE a.environment_id=j.environment_id AND a.job_id=j.id AND a.provider_message_id IS NOT NULL AND a.provider_message_id<>''),
      'attemptState',a.state,'providerIdKnown',coalesce(length(a.provider_message_id)>0,false),
      'lastCompletedScanAt',(SELECT h.last_completed_at::text FROM public.novu_rust_twilio_projection_heartbeat h WHERE h.environment_id=p_environment))
    INTO result FROM public.novu_queue_jobs j LEFT JOIN public.novu_rust_twilio_attempts a ON a.environment_id=j.environment_id AND a.job_id=j.id
    WHERE j.id=p_job AND j.environment_id=p_environment AND j.kind='experimental_twilio_sms';
  END IF;
  IF result IS NULL THEN RAISE EXCEPTION 'channel job not found' USING ERRCODE='22023'; END IF;
  RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_channel_result(text,uuid,text) FROM PUBLIC;
