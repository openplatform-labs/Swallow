-- Scoped investigation of pending Discord retries; no payload, token or endpoint URL.
CREATE INDEX novu_rust_discord_retry_pending_list_idx ON public.novu_queue_jobs(environment_id,id)
 WHERE kind IN ('experimental_discord_webhook','experimental_discord_workflow') AND status='pending';
CREATE FUNCTION public.novu_rust_discord_retry_list(p_env text,p_grace integer,p_overdue_only boolean,p_after uuid)
RETURNS TABLE(job_id uuid,kind text,destination_id text,available_at text,attempts integer,retry_count bigint,last_delay_seconds integer,last_retry_after_seconds integer,overdue boolean)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_grace IS NULL OR p_grace NOT BETWEEN 1 AND 86400 OR p_overdue_only IS NULL THEN RAISE EXCEPTION 'invalid retry list request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_assert_alert_view(p_env);
 RETURN QUERY
 SELECT j.id,j.kind,h.destination_id,j.available_at::text,j.attempts,h.retry_count,h.scheduled_delay_seconds,h.retry_after_seconds,
        j.available_at<=statement_timestamp()-make_interval(secs=>p_grace::double precision)
 FROM public.novu_queue_jobs j
 JOIN LATERAL (
   SELECT count(*) OVER () AS retry_count,r.destination_id,r.scheduled_delay_seconds,r.retry_after_seconds
   FROM public.novu_rust_discord_retry_history r WHERE r.job_id=j.id AND r.environment_id=p_env
   ORDER BY r.id DESC LIMIT 1
 ) h ON true
 WHERE j.environment_id=p_env AND j.kind IN ('experimental_discord_webhook','experimental_discord_workflow') AND j.status='pending'
 AND (p_after IS NULL OR j.id>p_after)
 AND (NOT p_overdue_only OR j.available_at<=statement_timestamp()-make_interval(secs=>p_grace::double precision))
 ORDER BY j.id LIMIT 101;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_discord_retry_list(text,integer,boolean,uuid) FROM PUBLIC;
