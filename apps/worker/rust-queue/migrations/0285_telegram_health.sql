-- Aggregate-only environment-scoped Telegram health for authorized operators.
CREATE INDEX IF NOT EXISTS novu_rust_telegram_attention_idx
 ON public.novu_rust_telegram_attempts(environment_id,state,job_id)
 WHERE state IN ('unknown','rejected','dispatching');
CREATE INDEX IF NOT EXISTS novu_rust_telegram_failed_jobs_idx
 ON public.novu_queue_jobs(environment_id,id)
 WHERE kind IN ('experimental_telegram','experimental_telegram_workflow') AND status='failed';
CREATE FUNCTION public.novu_rust_telegram_health(p_env text)
RETURNS TABLE(unknown_attempts bigint,rejected_attempts bigint,stranded_dispatches bigint,pre_dispatch_failures bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 PERFORM public.novu_rust_assert_alert_view(p_env);
 RETURN QUERY SELECT
   count(*) FILTER (WHERE a.state='unknown'),
   count(*) FILTER (WHERE a.state='rejected'),
   count(*) FILTER (WHERE a.state='dispatching' AND (j.status<>'running' OR j.lease_until IS NULL OR j.lease_until<=statement_timestamp())),
   (SELECT count(*) FROM public.novu_queue_jobs x WHERE x.environment_id=p_env
     AND x.kind IN ('experimental_telegram','experimental_telegram_workflow') AND x.status='failed'
     AND NOT EXISTS(SELECT 1 FROM public.novu_rust_telegram_attempts z WHERE z.job_id=x.id AND z.environment_id=x.environment_id))
 FROM public.novu_rust_telegram_attempts a JOIN public.novu_queue_jobs j ON j.id=a.job_id AND j.environment_id=a.environment_id
 WHERE a.environment_id=p_env AND j.kind IN ('experimental_telegram','experimental_telegram_workflow');
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_telegram_health(text) FROM PUBLIC;
