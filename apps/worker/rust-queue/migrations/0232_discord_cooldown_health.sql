-- This marker is written only when a worker defers a specific job.
CREATE INDEX novu_rust_discord_cooldown_deferred_idx ON public.novu_queue_jobs(environment_id,available_at,id)
 WHERE status='pending' AND last_error='Discord shared cooldown active' AND kind IN ('experimental_discord_webhook','experimental_discord_workflow');
CREATE FUNCTION public.novu_rust_discord_cooldown_health(p_env text,p_grace integer)
RETURNS TABLE(active_destinations bigint,deferred bigint,overdue bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_grace IS NULL OR p_grace NOT BETWEEN 1 AND 86400 THEN RAISE EXCEPTION 'invalid grace' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_assert_alert_view(p_env);
 RETURN QUERY SELECT
  (SELECT count(*) FROM public.novu_rust_discord_shared_cooldown c WHERE c.environment_id=p_env AND c.not_before>statement_timestamp()),
  count(j.id),count(j.id) FILTER (WHERE j.available_at<=statement_timestamp()-make_interval(secs=>p_grace::double precision))
 FROM public.novu_queue_jobs j WHERE j.environment_id=p_env AND j.status='pending' AND j.last_error='Discord shared cooldown active' AND j.kind IN ('experimental_discord_webhook','experimental_discord_workflow');
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_discord_cooldown_health(text,integer) FROM PUBLIC;
