-- Retrieve the current failed snapshot even after acknowledgment.
CREATE FUNCTION public.novu_rust_discord_failure_lookup(p_env text,p_job uuid)
RETURNS TABLE(job_id uuid,kind text,status text,attempt_state text,attempt_revision uuid,failure_revision bigint,attempts integer,http_status integer,retry_after_seconds integer,retry_not_before text,reviewed boolean)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_env IS NULL OR length(btrim(p_env))=0 OR p_job IS NULL THEN RAISE EXCEPTION 'invalid Discord lookup' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_assert_alert_view(p_env);
 RETURN QUERY SELECT j.id,j.kind,j.status,a.state,a.revision,j.failure_revision,j.attempts,a.http_status,a.retry_after_seconds,
  CASE WHEN a.http_status=429 AND a.retry_after_seconds BETWEEN 0 AND 86400 THEN to_char((a.updated_at+make_interval(secs=>a.retry_after_seconds::double precision)) AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') ELSE NULL END,
  EXISTS(SELECT 1 FROM public.novu_rust_discord_reviews r WHERE r.environment_id=j.environment_id AND r.job_id=j.id AND r.attempt_revision IS NOT DISTINCT FROM a.revision AND r.attempt_state IS NOT DISTINCT FROM a.state AND r.job_failure_revision=j.failure_revision AND r.job_attempts=j.attempts AND r.job_error=coalesce(j.last_error,''))
 FROM public.novu_queue_jobs j LEFT JOIN public.novu_rust_discord_attempts a ON a.job_id=j.id AND a.environment_id=j.environment_id
 WHERE j.environment_id=p_env AND j.id=p_job AND j.kind IN ('experimental_discord_webhook','experimental_discord_workflow') AND j.status='failed' AND (a.state IN ('unknown','rejected') OR a.job_id IS NULL);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_discord_failure_lookup(text,uuid) FROM PUBLIC;
