-- Aggregate-only, environment-scoped monitoring of the optional Slack guard.
CREATE INDEX novu_rust_slack_cooldown_deferred_idx ON public.novu_queue_jobs(environment_id,available_at,id)
 WHERE status='pending' AND last_error='Slack shared cooldown active';
CREATE FUNCTION public.novu_rust_slack_cooldown_health(p_env text,p_grace integer)
RETURNS TABLE(active boolean,not_before text,fallback_seconds integer,deferred bigint,overdue bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_grace IS NULL OR p_grace NOT BETWEEN 1 AND 86400 THEN RAISE EXCEPTION 'invalid grace' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_assert_alert_view(p_env);
 IF NOT EXISTS(SELECT 1 FROM public.novu_rust_slack_cooldown_policies p WHERE p.environment_id=p_env) THEN RAISE EXCEPTION 'Slack cooldown policy missing' USING ERRCODE='22023'; END IF;
 RETURN QUERY SELECT p.not_before>statement_timestamp(),CASE WHEN isfinite(p.not_before) THEN p.not_before::text ELSE NULL END,p.fallback_seconds,
  count(j.id),count(j.id) FILTER (WHERE j.available_at<=statement_timestamp()-p_grace*interval '1 second')
 FROM public.novu_rust_slack_cooldown_policies p LEFT JOIN public.novu_queue_jobs j ON j.environment_id=p.environment_id AND j.status='pending' AND j.last_error='Slack shared cooldown active' AND j.kind IN ('experimental_slack_chat','experimental_slack_webhook','experimental_slack_workflow_delivery')
 WHERE p.environment_id=p_env GROUP BY p.not_before,p.fallback_seconds;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_slack_cooldown_health(text,integer) FROM PUBLIC;
