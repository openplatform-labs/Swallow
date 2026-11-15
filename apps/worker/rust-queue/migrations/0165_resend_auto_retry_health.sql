-- Aggregate-only health for rate-limited or blocked automatic Resend retries.
CREATE FUNCTION public.novu_rust_resend_auto_retry_health(p_environment text,p_grace integer)
RETURNS TABLE(rate_limited bigint,exhausted bigint,guard_deferred bigint,overdue_auto bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_grace IS NULL OR p_grace NOT BETWEEN 1 AND 86400 THEN RAISE EXCEPTION 'invalid grace' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_assert_alert_view(p_environment);
 RETURN QUERY SELECT
  count(*) FILTER(WHERE j.status='failed' AND a.state='rejected' AND a.http_status=429 AND a.updated_at<=statement_timestamp()-p_grace*interval '1 second'),
  count(*) FILTER(WHERE j.status='failed' AND a.state='rejected' AND a.http_status=429 AND a.updated_at<=statement_timestamp()-p_grace*interval '1 second'
   AND p.enabled AND a.created_at>=p.not_before AND (SELECT count(*) FROM public.novu_rust_delivery_reconciliations r WHERE r.environment_id=p_environment AND r.job_id=j.id AND r.target_index IS NULL AND r.decision='retry')>=p.max_retries),
  count(*) FILTER(WHERE j.status='pending' AND j.last_error='Resend automatic retry requires evidence guards'),
  count(*) FILTER(WHERE j.status='pending' AND j.available_at<=statement_timestamp()-p_grace*interval '1 second' AND coalesce(last_retry.resend_auto_retry,false))
 FROM public.novu_queue_jobs j
 LEFT JOIN public.novu_rust_resend_attempts a ON a.job_id=j.id AND a.environment_id=j.environment_id
 LEFT JOIN public.novu_rust_resend_auto_retry_policy p ON p.environment_id=j.environment_id
 LEFT JOIN LATERAL (SELECT r.resend_auto_retry FROM public.novu_rust_delivery_reconciliations r WHERE r.environment_id=j.environment_id AND r.job_id=j.id AND r.target_index IS NULL AND r.decision='retry' ORDER BY r.decision_order DESC LIMIT 1) last_retry ON true
 WHERE j.environment_id=p_environment AND j.kind='experimental_resend_email' AND j.status IN ('pending','failed');
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_auto_retry_health(text,integer) FROM PUBLIC;
