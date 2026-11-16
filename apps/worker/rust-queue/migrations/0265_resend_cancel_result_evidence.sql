-- Show cancellation evidence alongside, without changing the delivery projection.
-- Add auto-correlation status to the existing environment-scoped channel view.
-- No provider IDs, correlation tags, addresses or webhook bodies are returned.
CREATE OR REPLACE FUNCTION public.novu_rust_channel_result(p_environment text,p_job uuid,p_provider text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb;
BEGIN
  PERFORM public.novu_rust_assert_alert_view(p_environment);
  IF p_job IS NULL OR p_provider IS NULL OR p_provider NOT IN ('resend','twilio') THEN
    RAISE EXCEPTION 'invalid job or provider' USING ERRCODE='22023';
  END IF;
  IF p_provider='resend' THEN
    SELECT jsonb_build_object('jobId',j.id,'jobStatus',j.status,
      'result',CASE WHEN v.conflict OR v.ambiguous THEN 'unknown' ELSE coalesce(j.resend_delivery_result,'unknown') END,
      'providerIdAmbiguous',v.ambiguous,'attemptState',a.state,'providerIdKnown',coalesce(length(a.provider_message_id)>0,false),
      'autoCorrelated',c.job_id IS NOT NULL,'autoCorrelationConflict',v.conflict,'autoCorrelatedAt',c.recorded_at::text,
      'cancellationState',cr.state,
      'cancellationConfirmationSource',CASE WHEN cr.state='confirmed' THEN CASE WHEN rr.request_id IS NOT NULL THEN 'operator' ELSE 'provider_response' END ELSE NULL END,
      'cancellationEvidence',CASE WHEN cr.state IS DISTINCT FROM 'confirmed' THEN NULL
        WHEN v.conflict OR v.ambiguous OR a.provider_message_id IS DISTINCT FROM cr.provider_id THEN 'ambiguous'
        WHEN EXISTS(SELECT 1 FROM public.novu_rust_resend_delivery_events e WHERE e.environment_id=j.environment_id AND e.email_id=cr.provider_id AND e.event_type IN ('email.sent','email.delivered','email.bounced','email.failed','email.suppressed','email.complained')) THEN 'conflicting'
        ELSE 'no_conflict_observed' END,
      'lastCompletedScanAt',(SELECT h.last_completed_at::text FROM public.novu_rust_resend_projection_heartbeat h WHERE h.environment_id=p_environment))
    INTO result FROM public.novu_queue_jobs j
      LEFT JOIN public.novu_rust_resend_attempts a ON a.environment_id=j.environment_id AND a.job_id=j.id
      LEFT JOIN public.novu_rust_resend_auto_correlations c ON c.environment_id=j.environment_id AND c.job_id=j.id
      LEFT JOIN public.novu_rust_resend_cancel_requests cr ON cr.environment_id=j.environment_id AND cr.job_id=j.id AND cr.state IN ('unknown','confirmed')
      LEFT JOIN public.novu_rust_resend_cancel_resolutions rr ON rr.environment_id=cr.environment_id AND rr.request_id=cr.request_id
      CROSS JOIN LATERAL (SELECT
        CASE WHEN c.job_id IS NOT NULL THEN public.novu_rust_resend_auto_evidence_conflicts(j.environment_id,j.id) ELSE false END AS conflict,
        EXISTS(SELECT 1 FROM public.novu_rust_resend_attempts b WHERE b.environment_id=j.environment_id AND b.provider_message_id=a.provider_message_id AND b.job_id<>a.job_id AND a.provider_message_id IS NOT NULL AND a.provider_message_id<>'') AS ambiguous) v
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

