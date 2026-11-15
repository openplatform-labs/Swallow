-- Aggregate monitoring for the optional at-least-once recovery worker.
CREATE FUNCTION public.novu_rust_smtp_auto_retry_health(p_env text,p_grace integer)
RETURNS TABLE(enabled boolean,eligible bigint,overdue bigint,exhausted bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_grace IS NULL OR p_grace NOT BETWEEN 1 AND 86400 THEN RAISE EXCEPTION 'invalid grace' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_assert_alert_view(p_env);
 RETURN QUERY
 WITH policy AS MATERIALIZED (SELECT * FROM public.novu_rust_smtp_auto_retry_policy WHERE environment_id=p_env),
 candidates AS MATERIALIZED (
  SELECT a.updated_at,p.delay_seconds,p.max_retries,
    (SELECT count(*) FROM public.novu_rust_smtp_auto_retries h WHERE h.environment_id=p_env AND h.job_id=j.id) AS retries
  FROM policy p JOIN public.novu_queue_jobs j ON j.environment_id=p.environment_id
  JOIN public.novu_rust_smtp_attempts a ON a.environment_id=j.environment_id AND a.job_id=j.id
  WHERE p.enabled AND j.kind='experimental_smtp_email' AND j.status='failed' AND a.state='unknown'
    AND a.updated_at>=p.eligible_since
    AND j.attachment_scrubbed_at IS NULL AND j.alternative_scrubbed_at IS NULL AND j.tracking_scrubbed_at IS NULL
    AND NOT EXISTS (SELECT 1 FROM public.novu_rust_smtp_reviews r WHERE r.environment_id=j.environment_id AND r.job_id=j.id AND r.attempt_revision=a.revision AND r.attempt_state=a.state AND r.job_failure_revision=j.failure_revision AND r.job_attempts=j.attempts AND r.job_error=coalesce(j.last_error,''))
 )
 SELECT p.enabled,
   count(*) FILTER (WHERE c.retries<c.max_retries AND c.updated_at<=statement_timestamp()-c.delay_seconds*interval '1 second'),
   count(*) FILTER (WHERE c.retries<c.max_retries AND c.updated_at<=statement_timestamp()-(c.delay_seconds+p_grace)*interval '1 second'),
   count(*) FILTER (WHERE c.retries>=c.max_retries)
 FROM policy p LEFT JOIN candidates c ON true GROUP BY p.enabled;
 IF NOT FOUND THEN RAISE EXCEPTION 'SMTP auto policy missing' USING ERRCODE='55000'; END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_smtp_auto_retry_health(text,integer) FROM PUBLIC;
