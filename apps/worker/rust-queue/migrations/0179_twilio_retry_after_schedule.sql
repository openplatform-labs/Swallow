-- Operator-selected Twilio 429 retry; immediate unknown-outcome path is unchanged.
ALTER TABLE public.novu_rust_delivery_reconciliations ADD COLUMN twilio_retry_not_before timestamptz;
CREATE FUNCTION public.novu_rust_resolve_twilio_after(p_env text,p_job uuid,p_dispatch uuid,p_operator text,p_reason text)
RETURNS timestamptz LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_until timestamptz; v_scheduled timestamptz;
BEGIN
 IF p_env IS NULL OR length(btrim(p_env))=0 OR p_job IS NULL OR p_dispatch IS NULL OR p_operator IS NULL OR length(btrim(p_operator))=0 OR octet_length(p_operator)>128 OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN
  RAISE EXCEPTION 'invalid SMS resolution' USING ERRCODE='22023';
 END IF;
 IF NOT public.novu_rust_lock_operator_grant(p_env,'retry') THEN RAISE EXCEPTION 'SMS resolution permission denied' USING ERRCODE='42501'; END IF;
 SELECT a.retry_not_before INTO v_until FROM public.novu_queue_jobs j JOIN public.novu_rust_twilio_attempts a ON a.job_id=j.id AND a.environment_id=j.environment_id
 WHERE j.id=p_job AND j.environment_id=p_env AND j.kind='experimental_twilio_sms' AND j.status='failed'
 AND a.dispatch_id=p_dispatch AND a.state='rejected' AND a.http_status=429 AND a.retry_after_seconds IS NOT NULL AND a.retry_not_before IS NOT NULL FOR UPDATE OF j,a;
 IF NOT FOUND THEN RAISE EXCEPTION 'current Twilio 429 with retry guidance required' USING ERRCODE='22023'; END IF;
 IF EXISTS (SELECT 1 FROM public.novu_rust_twilio_delivery_events e WHERE e.environment_id=p_env AND e.job_id=p_job AND (e.dispatch_id IS NULL OR e.dispatch_id=p_dispatch)) THEN
  RAISE EXCEPTION 'SMS retry refused: authenticated provider callback already exists' USING ERRCODE='22023';
 END IF;
 INSERT INTO public.novu_rust_delivery_reconciliations(id,environment_id,job_id,target_index,decision,operator_name,reason,previous_http_status,retry_dispatch_id,database_user,database_session_user,twilio_retry_not_before)
 VALUES(gen_random_uuid(),p_env,p_job,NULL,'retry',p_operator,p_reason,429,p_dispatch,session_user,session_user,v_until);
 DELETE FROM public.novu_rust_twilio_attempts WHERE environment_id=p_env AND job_id=p_job AND dispatch_id=p_dispatch;
 UPDATE public.novu_rust_sms_messages SET twilio_delivery_result=NULL,delivery_state='not_attempted',status='sent',error_id=NULL,error_text=NULL,identifier=NULL WHERE environment_id=p_env AND sms_job_id=p_job;
 UPDATE public.novu_queue_jobs SET status='pending',available_at=greatest(v_until,clock_timestamp()),last_error=NULL,twilio_delivery_result=NULL WHERE environment_id=p_env AND id=p_job AND status='failed' RETURNING available_at INTO v_scheduled;
 IF NOT FOUND THEN RAISE EXCEPTION 'SMS retry job not pending' USING ERRCODE='55000'; END IF;
 RETURN v_scheduled;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resolve_twilio_after(text,uuid,uuid,text,text) FROM PUBLIC;
-- The retry-authorized maintenance login can retrieve only eligible current guidance.
CREATE FUNCTION public.novu_rust_twilio_retry_guidance(p_env text,p_job uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE value jsonb;
BEGIN
 IF p_env IS NULL OR length(btrim(p_env))=0 OR p_job IS NULL THEN RAISE EXCEPTION 'invalid SMS guidance request' USING ERRCODE='22023'; END IF;
 IF NOT public.novu_rust_lock_operator_grant(p_env,'retry') THEN RAISE EXCEPTION 'SMS resolution permission denied' USING ERRCODE='42501'; END IF;
 SELECT jsonb_build_object('dispatchId',a.dispatch_id,'retryAfterSeconds',a.retry_after_seconds,'retryNotBefore',a.retry_not_before::text) INTO value
 FROM public.novu_queue_jobs j JOIN public.novu_rust_twilio_attempts a ON a.job_id=j.id AND a.environment_id=j.environment_id
 WHERE j.id=p_job AND j.environment_id=p_env AND j.kind='experimental_twilio_sms' AND j.status='failed' AND a.state='rejected' AND a.http_status=429 AND a.dispatch_id IS NOT NULL AND a.retry_after_seconds IS NOT NULL AND a.retry_not_before IS NOT NULL;
 IF value IS NULL THEN RAISE EXCEPTION 'current Twilio 429 with retry guidance required' USING ERRCODE='22023'; END IF;
 RETURN value;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_twilio_retry_guidance(text,uuid) FROM PUBLIC;
