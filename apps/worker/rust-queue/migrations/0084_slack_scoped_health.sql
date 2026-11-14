-- Aggregate-only health for an environment authorized to the session login.
CREATE OR REPLACE FUNCTION public.novu_rust_slack_health(p_environment text)
RETURNS TABLE(unknown_attempts bigint,rejected_attempts bigint,stranded_dispatches bigint,pre_dispatch_failures bigint,missing_endpoints bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  IF p_environment IS NULL OR length(btrim(p_environment))=0 THEN
    RAISE EXCEPTION 'environment required' USING ERRCODE='22023';
  END IF;
  IF NOT public.novu_rust_lock_operator_grant(p_environment,'slack_view') THEN
    RAISE EXCEPTION 'Slack view permission denied' USING ERRCODE='42501';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM public.novu_rust_environments e WHERE e.id=p_environment) THEN
    RAISE EXCEPTION 'environment not found' USING ERRCODE='22023';
  END IF;
  RETURN QUERY
    SELECT count(*) FILTER (WHERE a.state='unknown'),
      count(*) FILTER (WHERE a.state='rejected'),
      count(*) FILTER (WHERE a.state='dispatching' AND (j.status<>'running' OR j.lease_until<=statement_timestamp())),
      (SELECT count(*) FROM public.novu_queue_jobs x WHERE x.environment_id=p_environment
        AND x.kind IN ('experimental_slack_workflow_fanout','experimental_slack_workflow_delivery')
        AND x.status='failed' AND x.last_error IS NOT NULL
        AND NOT EXISTS(SELECT 1 FROM public.novu_rust_slack_attempts a2 WHERE a2.job_id=x.id)
        AND NOT EXISTS(SELECT 1 FROM public.novu_rust_slack_workflow_acknowledgements z WHERE z.environment_id=x.environment_id AND z.job_id=x.id AND z.job_attempts=x.attempts AND z.job_status=x.status AND z.job_error=x.last_error)),
      (SELECT count(*) FROM public.novu_queue_jobs x WHERE x.environment_id=p_environment
        AND x.kind='experimental_slack_workflow_fanout' AND x.status='cancelled' AND x.last_error='no Slack endpoints'
        AND NOT EXISTS(SELECT 1 FROM public.novu_rust_slack_workflow_acknowledgements z WHERE z.environment_id=x.environment_id AND z.job_id=x.id AND z.job_attempts=x.attempts AND z.job_status=x.status AND z.job_error=x.last_error))
    FROM public.novu_rust_slack_attempts a JOIN public.novu_queue_jobs j ON j.id=a.job_id AND j.environment_id=a.environment_id
    WHERE a.environment_id=p_environment AND j.kind IN ('experimental_slack_chat','experimental_slack_webhook','experimental_slack_workflow_delivery');
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_slack_health(text) FROM PUBLIC;
