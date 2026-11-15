-- Record automatic decisions independently of the operator-supplied display name.
ALTER TABLE public.novu_rust_delivery_reconciliations ADD COLUMN IF NOT EXISTS resend_auto_retry boolean NOT NULL DEFAULT false;
-- 0161 used this fixed name and reason for its own decisions. A manually
-- spoofed record may be conservatively classified as automatic.
UPDATE public.novu_rust_delivery_reconciliations SET resend_auto_retry=true
 WHERE operator_name='auto:resend-429' AND reason='configured automatic 429 retry'
   AND decision='retry' AND previous_http_status=429 AND resend_retry_not_before IS NOT NULL AND NOT resend_auto_retry;

CREATE OR REPLACE FUNCTION public.novu_rust_resend_auto_retry(p_environment text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_max integer; v_since timestamptz; v_job uuid; v_audit uuid;
BEGIN
 IF p_environment IS NULL OR length(p_environment)=0 THEN
  RAISE EXCEPTION 'invalid environment' USING ERRCODE='22023';
 END IF;
 IF NOT public.novu_rust_lock_operator_grant(p_environment,'retry') THEN
  RAISE EXCEPTION 'reconciliation permission denied' USING ERRCODE='42501';
 END IF;
 SELECT max_retries,not_before INTO v_max,v_since FROM public.novu_rust_resend_auto_retry_policy
  WHERE environment_id=p_environment AND enabled FOR SHARE;
 IF NOT FOUND THEN RETURN false; END IF;
 SELECT j.id INTO v_job FROM public.novu_queue_jobs j
 JOIN public.novu_rust_resend_attempts a ON a.job_id=j.id AND a.environment_id=j.environment_id
 WHERE j.environment_id=p_environment AND j.kind='experimental_resend_email' AND j.status='failed'
  AND a.state='rejected' AND a.http_status=429 AND a.retry_after_seconds IS NOT NULL AND a.retry_not_before IS NOT NULL AND a.created_at>=v_since
  AND (SELECT count(*) FROM public.novu_rust_delivery_reconciliations r WHERE r.environment_id=p_environment AND r.job_id=j.id AND r.target_index IS NULL AND r.decision='retry') < v_max
  AND NOT EXISTS (SELECT 1 FROM public.novu_rust_delivery_reconciliations r WHERE r.environment_id=p_environment AND r.job_id=j.id AND r.target_index IS NULL AND (r.decision<>'retry' OR r.previous_http_status IS DISTINCT FROM 429))
  AND NOT EXISTS (SELECT 1 FROM public.novu_rust_resend_delivery_events e WHERE e.environment_id=p_environment AND ((a.provider_message_id IS NOT NULL AND a.provider_message_id<>'' AND e.email_id=a.provider_message_id) OR (a.correlation_id IS NOT NULL AND e.correlation_id=a.correlation_id)))
  AND NOT EXISTS (SELECT 1 FROM public.novu_rust_delivery_reconciliations r JOIN public.novu_rust_resend_delivery_events e ON e.environment_id=r.environment_id AND ((r.retry_provider_message_id IS NOT NULL AND r.retry_provider_message_id<>'' AND e.email_id=r.retry_provider_message_id) OR (r.retry_correlation_id IS NOT NULL AND e.correlation_id=r.retry_correlation_id)) WHERE r.environment_id=p_environment AND r.job_id=j.id AND r.target_index IS NULL AND r.decision='retry')
 ORDER BY a.retry_not_before,j.id FOR UPDATE OF j SKIP LOCKED LIMIT 1;
 IF v_job IS NULL THEN RETURN false; END IF;
 PERFORM public.novu_rust_resolve_resend_after(p_environment,v_job,'auto:resend-429','configured automatic 429 retry');
 SELECT id INTO v_audit FROM public.novu_rust_delivery_reconciliations WHERE environment_id=p_environment AND job_id=v_job AND target_index IS NULL ORDER BY decision_order DESC LIMIT 1;
 IF v_audit IS NULL THEN RAISE EXCEPTION 'missing automatic retry audit' USING ERRCODE='55000'; END IF;
 UPDATE public.novu_rust_delivery_reconciliations SET resend_auto_retry=true WHERE id=v_audit;
 RETURN true;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_auto_retry(text) FROM PUBLIC;

-- The caller must lock the queue job before checking, so another decision
-- cannot change the latest audit between this test and dispatch preparation.
CREATE OR REPLACE FUNCTION public.novu_rust_resend_auto_dispatch_allowed(
 p_environment text,p_job uuid,p_evidence_guard boolean,p_correlation boolean,
 p_correlation_guard boolean,p_retry_key boolean)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_auto boolean;
BEGIN
 IF p_environment IS NULL OR length(p_environment)=0 OR p_job IS NULL OR
    p_evidence_guard IS NULL OR p_correlation IS NULL OR p_correlation_guard IS NULL OR p_retry_key IS NULL THEN
  RAISE EXCEPTION 'invalid dispatch parameters' USING ERRCODE='22023';
 END IF;
 SELECT resend_auto_retry INTO v_auto FROM public.novu_rust_delivery_reconciliations
  WHERE environment_id=p_environment AND job_id=p_job AND target_index IS NULL AND decision='retry'
  ORDER BY decision_order DESC LIMIT 1;
 RETURN NOT coalesce(v_auto,false) OR (p_evidence_guard AND p_correlation AND p_correlation_guard AND p_retry_key);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_auto_dispatch_allowed(text,uuid,boolean,boolean,boolean,boolean) FROM PUBLIC;
