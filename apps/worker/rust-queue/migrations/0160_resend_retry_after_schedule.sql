-- Preserve the source delay in the existing reconciliation audit.
ALTER TABLE public.novu_rust_delivery_reconciliations
 ADD COLUMN IF NOT EXISTS resend_retry_not_before timestamptz;

-- This optional path schedules a reviewed 429 retry no earlier than the
-- validated provider guidance. The existing immediate retry remains available.
CREATE OR REPLACE FUNCTION public.novu_rust_resolve_resend_after(
 p_environment text,p_job uuid,p_operator text,p_reason text)
RETURNS timestamptz LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_until timestamptz; v_scheduled timestamptz; v_audit uuid;
BEGIN
 IF p_environment IS NULL OR length(p_environment)=0 OR p_job IS NULL THEN
  RAISE EXCEPTION 'invalid reconciliation parameters' USING ERRCODE='22023';
 END IF;
 IF NOT public.novu_rust_lock_operator_grant(p_environment,'retry') THEN
  RAISE EXCEPTION 'reconciliation permission denied' USING ERRCODE='42501';
 END IF;
 PERFORM 1 FROM public.novu_queue_jobs j WHERE j.environment_id=p_environment AND j.id=p_job
  AND j.kind='experimental_resend_email' AND j.status='failed' FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'failed Resend job not found' USING ERRCODE='22023'; END IF;
 SELECT a.retry_not_before INTO v_until FROM public.novu_rust_resend_attempts a
  WHERE a.environment_id=p_environment AND a.job_id=p_job AND a.state='rejected'
    AND a.http_status=429 AND a.retry_after_seconds IS NOT NULL AND a.retry_not_before IS NOT NULL FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION '429 attempt with retry guidance required' USING ERRCODE='22023'; END IF;
 -- Reuse existing evidence checks, permissions, atomic state transitions and audit.
 PERFORM public.novu_rust_resolve_email_sms(p_environment,p_job,'retry',p_operator,p_reason);
 SELECT id INTO v_audit FROM public.novu_rust_delivery_reconciliations
  WHERE environment_id=p_environment AND job_id=p_job AND target_index IS NULL
  ORDER BY decision_order DESC LIMIT 1;
 IF v_audit IS NULL THEN RAISE EXCEPTION 'missing reconciliation audit' USING ERRCODE='55000'; END IF;
 UPDATE public.novu_rust_delivery_reconciliations SET resend_retry_not_before=v_until WHERE id=v_audit;
 UPDATE public.novu_queue_jobs SET available_at=greatest(v_until,clock_timestamp())
  WHERE environment_id=p_environment AND id=p_job AND status='pending' RETURNING available_at INTO v_scheduled;
 IF NOT FOUND THEN RAISE EXCEPTION 'retry job not pending' USING ERRCODE='55000'; END IF;
 RETURN v_scheduled;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resolve_resend_after(text,uuid,text,text) FROM PUBLIC;
