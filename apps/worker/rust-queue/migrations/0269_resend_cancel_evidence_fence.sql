-- Refuse known delivery evidence or previous retry identity before cancellation.
CREATE OR REPLACE FUNCTION public.novu_rust_resend_cancel_reserve(p_env text,p_request uuid,p_job uuid,p_reason text) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_oid oid; v_old public.novu_rust_resend_cancel_requests%ROWTYPE; v_provider text; v_inserted boolean;
BEGIN
 IF p_request IS NULL OR p_job IS NULL OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>500 OR p_reason ~ '[[:cntrl:]]' THEN RAISE EXCEPTION 'invalid cancellation request' USING ERRCODE='22023'; END IF;
 v_oid:=public.novu_rust_resend_cancel_access(p_env,true);
 IF current_setting('transaction_isolation')<>'read committed' THEN RAISE EXCEPTION 'cancellation requires read committed isolation' USING ERRCODE='55000'; END IF;
 -- Serializes competing operations using the existing queue job lock.
 PERFORM 1 FROM public.novu_queue_jobs WHERE environment_id=p_env AND id=p_job FOR UPDATE;
 SELECT * INTO v_old FROM public.novu_rust_resend_cancel_requests WHERE environment_id=p_env AND request_id=p_request;
 IF FOUND THEN
  IF (v_old.job_id,v_old.reason,v_old.database_principal,v_old.principal_oid) IS DISTINCT FROM (p_job,p_reason,session_user,v_oid) THEN RAISE EXCEPTION 'request conflict' USING ERRCODE='23505'; END IF;
  RETURN jsonb_build_object('requestId',p_request,'state',v_old.state,'fresh',false);
 END IF;
 -- Coordinate with existing provider-ID writers, then re-read their committed evidence.
 SELECT a.provider_message_id INTO v_provider FROM public.novu_rust_resend_attempts a WHERE a.environment_id=p_env AND a.job_id=p_job;
 IF v_provider IS NOT NULL AND v_provider<>'' THEN
  PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('resend-provider/'||length(p_env)::text||'/'||p_env||'/'||v_provider,0));
 END IF;
 SELECT a.provider_message_id INTO v_provider FROM public.novu_queue_jobs j JOIN public.novu_rust_resend_attempts a ON a.environment_id=j.environment_id AND a.job_id=j.id
 WHERE j.environment_id=p_env AND j.id=p_job AND j.kind='experimental_resend_email' AND j.status='completed' AND j.resend_scheduled_at>clock_timestamp()
 AND (j.resend_delivery_result IS NULL OR j.resend_delivery_result='pending') AND a.state='accepted'
 AND a.provider_message_id=v_provider AND a.provider_message_id ~ '^[A-Za-z0-9_-]+$' AND octet_length(a.provider_message_id)<=1024
 AND NOT EXISTS(SELECT 1 FROM public.novu_rust_resend_attempts b WHERE b.environment_id=a.environment_id AND b.provider_message_id=a.provider_message_id AND b.job_id<>a.job_id)
 AND NOT EXISTS(SELECT 1 FROM public.novu_rust_delivery_reconciliations r WHERE r.environment_id=a.environment_id AND r.decision='retry' AND r.target_index IS NULL AND r.retry_provider_message_id=a.provider_message_id)
 AND NOT EXISTS(SELECT 1 FROM public.novu_rust_resend_delivery_events e WHERE e.environment_id=a.environment_id AND e.email_id=a.provider_message_id AND e.event_type IN ('email.sent','email.delivered','email.delivery_delayed','email.opened','email.clicked','email.bounced','email.failed','email.suppressed','email.complained'));
 IF NOT FOUND THEN RAISE EXCEPTION 'scheduled cancellation not eligible' USING ERRCODE='55000'; END IF;
 INSERT INTO public.novu_rust_resend_cancel_requests(environment_id,request_id,job_id,provider_id,database_principal,principal_oid,reason,state)
 VALUES(p_env,p_request,p_job,v_provider,session_user,v_oid,p_reason,'unknown') ON CONFLICT DO NOTHING RETURNING true INTO v_inserted;
 IF NOT coalesce(v_inserted,false) THEN RAISE EXCEPTION 'cancellation request or job conflict' USING ERRCODE='23505'; END IF;
 RETURN jsonb_build_object('requestId',p_request,'state','unknown','fresh',true,'providerId',v_provider);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_cancel_reserve(text,uuid,uuid,text) FROM PUBLIC;
-- Include delivery-attempt and engagement evidence in cancellation conflict monitoring.
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
        WHEN v.conflict OR v.ambiguous OR a.provider_message_id IS DISTINCT FROM cr.provider_id
          OR EXISTS(SELECT 1 FROM public.novu_rust_delivery_reconciliations r WHERE r.environment_id=j.environment_id AND r.decision='retry' AND r.target_index IS NULL AND r.retry_provider_message_id=cr.provider_id) THEN 'ambiguous'
        WHEN EXISTS(SELECT 1 FROM public.novu_rust_resend_delivery_events e WHERE e.environment_id=j.environment_id AND e.email_id=cr.provider_id AND e.event_type IN ('email.sent','email.delivered','email.delivery_delayed','email.opened','email.clicked','email.bounced','email.failed','email.suppressed','email.complained')) THEN 'conflicting'
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

