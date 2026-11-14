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
    SELECT jsonb_build_object('jobId',j.id,'jobStatus',j.status,'result',coalesce(j.resend_delivery_result,'unknown'),
      'attemptState',a.state,'providerIdKnown',coalesce(length(a.provider_message_id)>0,false),
      'lastCompletedScanAt',(SELECT h.last_completed_at::text FROM public.novu_rust_resend_projection_heartbeat h WHERE h.environment_id=p_environment))
    INTO result FROM public.novu_queue_jobs j LEFT JOIN public.novu_rust_resend_attempts a ON a.environment_id=j.environment_id AND a.job_id=j.id
    WHERE j.id=p_job AND j.environment_id=p_environment AND j.kind='experimental_resend_email';
  ELSE
    SELECT jsonb_build_object('jobId',j.id,'jobStatus',j.status,'result',coalesce(j.twilio_delivery_result,'unknown'),
      'attemptState',a.state,'providerIdKnown',coalesce(length(a.provider_message_id)>0,false),
      'lastCompletedScanAt',(SELECT h.last_completed_at::text FROM public.novu_rust_twilio_projection_heartbeat h WHERE h.environment_id=p_environment))
    INTO result FROM public.novu_queue_jobs j LEFT JOIN public.novu_rust_twilio_attempts a ON a.environment_id=j.environment_id AND a.job_id=j.id
    WHERE j.id=p_job AND j.environment_id=p_environment AND j.kind='experimental_twilio_sms';
  END IF;
  IF result IS NULL THEN RAISE EXCEPTION 'channel job not found' USING ERRCODE='22023'; END IF;
  RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_channel_result(text,uuid,text) FROM PUBLIC;
