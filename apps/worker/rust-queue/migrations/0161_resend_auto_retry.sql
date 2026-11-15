-- Opt-in policy is managed by a DB administrator, not by a worker.
CREATE TABLE IF NOT EXISTS public.novu_rust_resend_auto_retry_policy (
 environment_id text PRIMARY KEY REFERENCES public.novu_rust_environments(id),
 enabled boolean NOT NULL DEFAULT false,
 max_retries integer NOT NULL CHECK (max_retries BETWEEN 1 AND 10),
 not_before timestamptz NOT NULL DEFAULT clock_timestamp()
);
REVOKE ALL ON public.novu_rust_resend_auto_retry_policy FROM PUBLIC;

-- Reserve at most one failed 429 per call. The row lock serializes schedulers
-- and the inner function performs the existing evidence checks and audit.
CREATE OR REPLACE FUNCTION public.novu_rust_resend_auto_retry(p_environment text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_max integer; v_since timestamptz; v_job uuid;
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
 RETURN true;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_auto_retry(text) FROM PUBLIC;
