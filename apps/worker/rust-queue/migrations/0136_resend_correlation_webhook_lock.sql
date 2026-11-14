-- Lock all known jobs in UUID order, including retries whose attempt was removed.
CREATE OR REPLACE FUNCTION public.novu_rust_lock_resend_webhook_jobs_with_correlation(
  p_environment text,p_email_id text,p_correlation uuid)
RETURNS integer LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE n integer;
BEGIN
  IF p_environment IS NULL OR length(btrim(p_environment))=0 OR p_email_id IS NULL
     OR length(p_email_id)=0 OR octet_length(p_email_id)>256 THEN
    RAISE EXCEPTION 'invalid callback scope' USING ERRCODE='22023';
  END IF;
  PERFORM 1 FROM public.novu_rust_resend_webhook_lock_grants g
    WHERE g.environment_id=p_environment AND g.database_principal=session_user
      AND g.principal_oid=(SELECT r.oid FROM pg_catalog.pg_roles r WHERE r.rolname=session_user AND r.rolcanlogin)
    FOR SHARE;
  IF NOT FOUND THEN RAISE EXCEPTION 'webhook lock permission denied' USING ERRCODE='42501'; END IF;
  PERFORM j.id FROM public.novu_queue_jobs j
    WHERE j.environment_id=p_environment AND j.kind='experimental_resend_email' AND j.id IN (
      SELECT a.job_id FROM public.novu_rust_resend_attempts a WHERE a.environment_id=p_environment
        AND (a.provider_message_id=p_email_id OR (p_correlation IS NOT NULL AND a.correlation_id=p_correlation))
      UNION
      SELECT r.job_id FROM public.novu_rust_delivery_reconciliations r
        WHERE r.environment_id=p_environment AND r.target_index IS NULL AND r.decision='retry'
          AND (r.retry_provider_message_id=p_email_id OR (p_correlation IS NOT NULL AND r.retry_correlation_id=p_correlation)))
    ORDER BY j.id FOR UPDATE;
  GET DIAGNOSTICS n = ROW_COUNT;
  RETURN n;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_lock_resend_webhook_jobs_with_correlation(text,text,uuid) FROM PUBLIC;
