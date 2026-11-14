-- Record each new Slack workflow failure or missing-endpoint transition.
ALTER TABLE public.novu_queue_jobs ADD COLUMN IF NOT EXISTS slack_attention_revision bigint NOT NULL DEFAULT 0;
ALTER TABLE public.novu_rust_slack_workflow_acknowledgements ADD COLUMN IF NOT EXISTS job_attention_revision bigint NOT NULL DEFAULT 0;
CREATE OR REPLACE FUNCTION public.novu_rust_slack_attention_revision() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF NEW.kind IN ('experimental_slack_workflow_fanout','experimental_slack_workflow_delivery') AND NEW.status IN ('failed','cancelled')
   AND (OLD.kind IS DISTINCT FROM NEW.kind OR OLD.status IS DISTINCT FROM NEW.status OR OLD.attempts IS DISTINCT FROM NEW.attempts OR OLD.last_error IS DISTINCT FROM NEW.last_error) THEN
   NEW.slack_attention_revision:=OLD.slack_attention_revision+1;
 END IF;
 RETURN NEW;
END; $$;
DROP TRIGGER IF EXISTS novu_rust_slack_attention_revision_trigger ON public.novu_queue_jobs;
CREATE TRIGGER novu_rust_slack_attention_revision_trigger BEFORE UPDATE OF kind,status,attempts,last_error ON public.novu_queue_jobs FOR EACH ROW EXECUTE FUNCTION public.novu_rust_slack_attention_revision();
DO $$
DECLARE v_name text;
BEGIN
 SELECT c.conname INTO v_name FROM pg_catalog.pg_constraint c WHERE c.conrelid='public.novu_rust_slack_workflow_acknowledgements'::regclass AND c.contype='u' AND pg_catalog.pg_get_constraintdef(c.oid)='UNIQUE (job_id, job_attempts, job_status, job_error)';
 IF v_name IS NOT NULL THEN EXECUTE pg_catalog.format('ALTER TABLE public.novu_rust_slack_workflow_acknowledgements DROP CONSTRAINT %I',v_name); END IF;
END; $$;
CREATE UNIQUE INDEX IF NOT EXISTS novu_rust_slack_ack_revision_idx ON public.novu_rust_slack_workflow_acknowledgements(job_id,job_attempts,job_status,job_error,job_attention_revision);

-- This function exposes only bounded non-payload metadata for an authorized environment.
CREATE OR REPLACE FUNCTION public.novu_rust_list_slack_attention(p_environment text,p_after uuid)
RETURNS TABLE(job_id uuid,kind text,job_status text,attempts integer,http_status integer,category text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
  IF p_environment IS NULL OR length(btrim(p_environment))=0 THEN
    RAISE EXCEPTION 'environment required' USING ERRCODE='22023';
  END IF;
  IF NOT public.novu_rust_lock_operator_grant(p_environment,'slack_view') THEN
    RAISE EXCEPTION 'Slack view permission denied' USING ERRCODE='42501';
  END IF;
  IF NOT EXISTS(SELECT 1 FROM public.novu_rust_environments e WHERE e.id=p_environment) THEN
    RAISE EXCEPTION 'environment not found' USING ERRCODE='22023';
  END IF;
  RETURN QUERY
    SELECT j.id,j.kind,j.status,j.attempts,a.http_status,
      CASE WHEN a.state='unknown' THEN 'unknownAttempt'
        WHEN a.state='rejected' THEN 'rejectedAttempt'
        WHEN a.state='dispatching' AND (j.status<>'running' OR j.lease_until<=statement_timestamp()) THEN 'strandedDispatch'
        WHEN j.status='failed' THEN 'preDispatchFailure' ELSE 'missingEndpoint' END
    FROM public.novu_queue_jobs j
    LEFT JOIN public.novu_rust_slack_attempts a ON a.job_id=j.id AND a.environment_id=j.environment_id
    WHERE j.environment_id=p_environment AND (p_after IS NULL OR j.id>p_after) AND (
      (j.kind IN ('experimental_slack_chat','experimental_slack_webhook','experimental_slack_workflow_delivery') AND
        (a.state IN ('unknown','rejected') OR (a.state='dispatching' AND (j.status<>'running' OR j.lease_until<=statement_timestamp()))))
      OR (j.kind IN ('experimental_slack_workflow_fanout','experimental_slack_workflow_delivery') AND j.status='failed' AND j.last_error IS NOT NULL
        AND NOT EXISTS(SELECT 1 FROM public.novu_rust_slack_attempts x WHERE x.job_id=j.id)
        AND NOT EXISTS(SELECT 1 FROM public.novu_rust_slack_workflow_acknowledgements z WHERE z.environment_id=j.environment_id AND z.job_id=j.id AND z.job_attempts=j.attempts AND z.job_status=j.status AND z.job_error=j.last_error AND z.job_attention_revision=j.slack_attention_revision))
      OR (j.kind='experimental_slack_workflow_fanout' AND j.status='cancelled' AND j.last_error='no Slack endpoints'
        AND NOT EXISTS(SELECT 1 FROM public.novu_rust_slack_workflow_acknowledgements z WHERE z.environment_id=j.environment_id AND z.job_id=j.id AND z.job_attempts=j.attempts AND z.job_status=j.status AND z.job_error=j.last_error AND z.job_attention_revision=j.slack_attention_revision))
    ) ORDER BY j.id LIMIT 100;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_list_slack_attention(text,uuid) FROM PUBLIC;

-- Restrict acknowledgements to a single audited operation with no direct table grants.
CREATE OR REPLACE FUNCTION public.novu_rust_ack_slack_workflow(
    p_environment text,p_job uuid,p_operator text,p_reason text
) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_attempts integer; v_status text; v_error text; v_revision bigint; v_inserted integer;
BEGIN
    IF p_environment IS NULL OR length(btrim(p_environment))=0 OR p_job IS NULL
       OR p_operator IS NULL OR length(btrim(p_operator))=0 OR octet_length(p_operator)>128
       OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN
        RAISE EXCEPTION 'invalid Slack acknowledgement' USING ERRCODE='22023';
    END IF;
    IF NOT public.novu_rust_lock_operator_grant(p_environment,'slack_ack') THEN
        RAISE EXCEPTION 'Slack acknowledgement permission denied' USING ERRCODE='42501';
    END IF;
    SELECT j.attempts,j.status,j.last_error,j.slack_attention_revision INTO v_attempts,v_status,v_error,v_revision
      FROM public.novu_queue_jobs j
      WHERE j.environment_id=p_environment AND j.id=p_job
        AND j.kind IN ('experimental_slack_workflow_fanout','experimental_slack_workflow_delivery')
      FOR UPDATE OF j;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Slack workflow job not found' USING ERRCODE='22023';
    END IF;
    IF v_error IS NULL OR NOT (v_status='failed' OR (v_status='cancelled' AND v_error='no Slack endpoints'))
       OR EXISTS(SELECT 1 FROM public.novu_rust_slack_attempts a WHERE a.job_id=p_job) THEN
        RAISE EXCEPTION 'Slack workflow job is not eligible for acknowledgement' USING ERRCODE='22023';
    END IF;
    INSERT INTO public.novu_rust_slack_workflow_acknowledgements
      (id,environment_id,job_id,job_attempts,job_status,job_error,job_attention_revision,operator_name,reason)
      VALUES (gen_random_uuid(),p_environment,p_job,v_attempts,v_status,v_error,v_revision,p_operator,p_reason)
      ON CONFLICT(job_id,job_attempts,job_status,job_error,job_attention_revision) DO NOTHING;
    GET DIAGNOSTICS v_inserted=ROW_COUNT;
    RETURN v_inserted=1;
END;
$$;
REVOKE ALL ON FUNCTION public.novu_rust_ack_slack_workflow(text,uuid,text,text) FROM PUBLIC;

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
        AND NOT EXISTS(SELECT 1 FROM public.novu_rust_slack_workflow_acknowledgements z WHERE z.environment_id=x.environment_id AND z.job_id=x.id AND z.job_attempts=x.attempts AND z.job_status=x.status AND z.job_error=x.last_error AND z.job_attention_revision=x.slack_attention_revision)),
      (SELECT count(*) FROM public.novu_queue_jobs x WHERE x.environment_id=p_environment
        AND x.kind='experimental_slack_workflow_fanout' AND x.status='cancelled' AND x.last_error='no Slack endpoints'
        AND NOT EXISTS(SELECT 1 FROM public.novu_rust_slack_workflow_acknowledgements z WHERE z.environment_id=x.environment_id AND z.job_id=x.id AND z.job_attempts=x.attempts AND z.job_status=x.status AND z.job_error=x.last_error AND z.job_attention_revision=x.slack_attention_revision))
    FROM public.novu_rust_slack_attempts a JOIN public.novu_queue_jobs j ON j.id=a.job_id AND j.environment_id=a.environment_id
    WHERE a.environment_id=p_environment AND j.kind IN ('experimental_slack_chat','experimental_slack_webhook','experimental_slack_workflow_delivery');
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_slack_health(text) FROM PUBLIC;
