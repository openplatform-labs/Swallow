-- Only aggregate environment-scoped counts; no destination or subscriber data.
CREATE FUNCTION public.novu_rust_discord_retry_health(p_env text,p_grace integer)
RETURNS TABLE(pending bigint,overdue bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_grace IS NULL OR p_grace NOT BETWEEN 1 AND 86400 THEN RAISE EXCEPTION 'invalid retry grace' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_assert_alert_view(p_env);
 RETURN QUERY SELECT count(*),count(*) FILTER (WHERE j.available_at<=statement_timestamp()-make_interval(secs=>p_grace::double precision))
 FROM public.novu_queue_jobs j
 WHERE j.environment_id=p_env AND j.kind IN ('experimental_discord_webhook','experimental_discord_workflow') AND j.status='pending'
 AND EXISTS(SELECT 1 FROM public.novu_rust_discord_retry_history h WHERE h.job_id=j.id AND h.environment_id=p_env);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_discord_retry_health(text,integer) FROM PUBLIC;
