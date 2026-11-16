-- Match the pre-dispatch health count while exposing no payload or arbitrary error text.
CREATE FUNCTION public.novu_rust_telegram_predispatch_list(p_env text,p_after uuid) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb;
BEGIN
 PERFORM public.novu_rust_telegram_operator_access(p_env,'view');
 WITH candidates AS MATERIALIZED (
  SELECT j.id,j.kind,j.attempts,j.failure_revision,
    CASE WHEN j.last_error IN ('invalid Telegram job','invalid Telegram workflow job','subscriber not found','subscriber identity changed','invalid subscriber profile','Telegram workflow step changed or disabled','Telegram endpoint changed or deleted','actor identity changed or missing','unsupported Telegram template or input','invalid Telegram request') THEN j.last_error ELSE 'other' END AS diagnostic
  FROM public.novu_queue_jobs j
  WHERE j.environment_id=p_env AND j.kind IN ('experimental_telegram','experimental_telegram_workflow') AND j.status='failed'
   AND NOT EXISTS (SELECT 1 FROM public.novu_rust_telegram_attempts a WHERE a.environment_id=j.environment_id AND a.job_id=j.id)
   AND (p_after IS NULL OR j.id>p_after)
  ORDER BY j.id LIMIT 51
 ), page AS MATERIALIZED (SELECT * FROM candidates ORDER BY id LIMIT 50)
 SELECT jsonb_build_object('data',COALESCE((SELECT jsonb_agg(jsonb_build_object('jobId',id,'jobKind',kind,'claims',attempts,'failureRevision',failure_revision,'diagnostic',diagnostic) ORDER BY id) FROM page),'[]'::jsonb),
  'hasMore',(SELECT count(*)>50 FROM candidates),'next',CASE WHEN (SELECT count(*)>50 FROM candidates) THEN (SELECT id FROM page ORDER BY id DESC LIMIT 1) ELSE NULL END) INTO result;
 RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_telegram_predispatch_list(text,uuid) FROM PUBLIC;
