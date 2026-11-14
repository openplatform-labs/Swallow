-- Refuse Resend retries when a known provider ID already has callback evidence.
-- Keep manual Resend acceptance and message history consistent.
-- Scoped reconciliation of uncertain email and SMS attempts. FCM remains on
-- the legacy path until target/group reconciliation is implemented here.
CREATE OR REPLACE FUNCTION public.novu_rust_resolve_email_sms(
  p_environment text,p_job uuid,p_decision text,p_operator text,p_reason text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_kind text; v_status text; v_http integer; v_dispatch uuid; v_provider_id text;
BEGIN
  IF p_environment IS NULL OR length(p_environment)=0 OR p_job IS NULL
     OR p_decision IS NULL OR p_decision NOT IN ('retry','accepted')
     OR p_operator IS NULL OR length(btrim(p_operator))=0 OR octet_length(p_operator)>128
     OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN
    RAISE EXCEPTION 'invalid reconciliation parameters' USING ERRCODE='22023';
  END IF;
  IF NOT public.novu_rust_lock_operator_grant(p_environment,p_decision) THEN
    RAISE EXCEPTION 'reconciliation permission denied' USING ERRCODE='42501';
  END IF;
  SELECT j.kind,j.status INTO v_kind,v_status FROM public.novu_queue_jobs j
    WHERE j.environment_id=p_environment AND j.id=p_job FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'job not found' USING ERRCODE='22023'; END IF;
  IF v_status <> 'failed' THEN RAISE EXCEPTION 'job must be failed and not running' USING ERRCODE='22023'; END IF;
  IF v_kind='experimental_sendgrid_email' THEN
    SELECT a.http_status,a.dispatch_id INTO v_http,v_dispatch FROM public.novu_rust_sendgrid_attempts a
      WHERE a.environment_id=p_environment AND a.job_id=p_job AND a.state='unknown' FOR UPDATE;
  ELSIF v_kind='experimental_resend_email' THEN
    SELECT a.http_status,NULL::uuid,a.provider_message_id INTO v_http,v_dispatch,v_provider_id FROM public.novu_rust_resend_attempts a
      WHERE a.environment_id=p_environment AND a.job_id=p_job AND a.state='unknown' FOR UPDATE;
  ELSIF v_kind='experimental_twilio_sms' THEN
    SELECT a.http_status,a.dispatch_id INTO v_http,v_dispatch FROM public.novu_rust_twilio_attempts a
      WHERE a.environment_id=p_environment AND a.job_id=p_job AND a.state='unknown' FOR UPDATE;
  ELSE
    RAISE EXCEPTION 'scoped reconciliation supports email and SMS only' USING ERRCODE='22023';
  END IF;
  IF NOT FOUND THEN RAISE EXCEPTION 'unknown attempt not found' USING ERRCODE='22023'; END IF;
  IF p_decision='retry' AND v_kind='experimental_sendgrid_email' AND EXISTS (
    SELECT 1 FROM public.novu_rust_sendgrid_delivery_events e
    JOIN public.novu_rust_email_messages m ON m.environment_id=e.environment_id AND m.internal_id=e.message_internal_id
    WHERE m.environment_id=p_environment AND m.email_job_id=p_job
      AND (v_dispatch IS NULL OR NOT e.dispatch_proof_verified OR e.dispatch_id IS NULL OR e.dispatch_id=v_dispatch)) THEN
    RAISE EXCEPTION 'email retry refused: authenticated provider callback already exists' USING ERRCODE='22023';
  END IF;
  IF p_decision='retry' AND v_kind='experimental_resend_email' AND v_provider_id IS NOT NULL AND v_provider_id<>'' AND EXISTS (
    SELECT 1 FROM public.novu_rust_resend_delivery_events e WHERE e.environment_id=p_environment AND e.email_id=v_provider_id) THEN
    RAISE EXCEPTION 'Resend retry refused: authenticated provider callback already exists' USING ERRCODE='22023';
  END IF;
  IF p_decision='retry' AND v_kind='experimental_twilio_sms' AND EXISTS (
    SELECT 1 FROM public.novu_rust_twilio_delivery_events e WHERE e.environment_id=p_environment AND e.job_id=p_job
      AND (v_dispatch IS NULL OR e.dispatch_id IS NULL OR e.dispatch_id=v_dispatch)) THEN
    RAISE EXCEPTION 'SMS retry refused: authenticated provider callback already exists' USING ERRCODE='22023';
  END IF;
  INSERT INTO public.novu_rust_delivery_reconciliations
    (id,environment_id,job_id,target_index,decision,operator_name,reason,previous_http_status,retry_dispatch_id,database_user,database_session_user)
    VALUES (gen_random_uuid(),p_environment,p_job,NULL,p_decision,p_operator,p_reason,v_http,
      CASE WHEN p_decision='retry' THEN v_dispatch ELSE NULL END,session_user,session_user);
  IF p_decision='retry' THEN
    IF v_kind='experimental_sendgrid_email' THEN
      DELETE FROM public.novu_rust_sendgrid_attempts WHERE environment_id=p_environment AND job_id=p_job;
      UPDATE public.novu_queue_jobs SET sendgrid_delivery_result='unknown' WHERE environment_id=p_environment AND id=p_job;
      UPDATE public.novu_rust_email_messages SET delivery_state='not_attempted',status='sent',error_id=NULL,error_text=NULL,identifier=NULL,sendgrid_delivery_result='unknown' WHERE environment_id=p_environment AND email_job_id=p_job;
    ELSIF v_kind='experimental_resend_email' THEN
      DELETE FROM public.novu_rust_resend_attempts WHERE environment_id=p_environment AND job_id=p_job;
      UPDATE public.novu_queue_jobs SET resend_delivery_result=NULL WHERE environment_id=p_environment AND id=p_job;
      UPDATE public.novu_rust_email_messages SET delivery_state='not_attempted',status='sent',error_id=NULL,error_text=NULL,identifier=NULL,resend_delivery_result=NULL WHERE environment_id=p_environment AND email_job_id=p_job;
    ELSE
      DELETE FROM public.novu_rust_twilio_attempts WHERE environment_id=p_environment AND job_id=p_job;
      UPDATE public.novu_queue_jobs SET twilio_delivery_result=NULL WHERE environment_id=p_environment AND id=p_job;
      UPDATE public.novu_rust_sms_messages SET twilio_delivery_result=NULL,delivery_state='not_attempted',status='sent',error_id=NULL,error_text=NULL,identifier=NULL WHERE environment_id=p_environment AND sms_job_id=p_job;
    END IF;
    UPDATE public.novu_queue_jobs SET status='pending',available_at=now(),last_error=NULL WHERE environment_id=p_environment AND id=p_job;
  ELSE
    IF v_kind='experimental_sendgrid_email' THEN
      UPDATE public.novu_rust_sendgrid_attempts SET state='accepted',updated_at=clock_timestamp() WHERE environment_id=p_environment AND job_id=p_job;
      UPDATE public.novu_rust_email_messages SET delivery_state='accepted',status='sent',error_id=NULL,error_text=NULL WHERE environment_id=p_environment AND email_job_id=p_job;
    ELSIF v_kind='experimental_resend_email' THEN
      UPDATE public.novu_rust_resend_attempts SET state='accepted',updated_at=clock_timestamp() WHERE environment_id=p_environment AND job_id=p_job;
      UPDATE public.novu_queue_jobs SET resend_delivery_result=NULL WHERE environment_id=p_environment AND id=p_job;
      UPDATE public.novu_rust_email_messages SET delivery_state='accepted',status='sent',error_id=NULL,error_text=NULL,identifier=coalesce((SELECT provider_message_id FROM public.novu_rust_resend_attempts WHERE environment_id=p_environment AND job_id=p_job),identifier),resend_delivery_result=NULL WHERE environment_id=p_environment AND email_job_id=p_job;
    ELSE
      UPDATE public.novu_rust_twilio_attempts SET state='accepted',updated_at=clock_timestamp() WHERE environment_id=p_environment AND job_id=p_job;
      UPDATE public.novu_rust_sms_messages SET delivery_state='accepted',status='sent',error_id=NULL,error_text=NULL WHERE environment_id=p_environment AND sms_job_id=p_job;
    END IF;
    UPDATE public.novu_queue_jobs SET status='completed',last_error=NULL WHERE environment_id=p_environment AND id=p_job;
  END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resolve_email_sms(text,uuid,text,text,text) FROM PUBLIC;
