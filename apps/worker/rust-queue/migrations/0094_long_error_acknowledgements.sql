-- Keep full diagnostics in the restricted audit row, not in B-tree keys.
-- UTF-8 conversion is deterministic for PostgreSQL text in a fixed database.
CREATE OR REPLACE FUNCTION public.novu_rust_failure_digest(p_error text) RETURNS bytea
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE SET search_path=pg_catalog AS $$
 SELECT pg_catalog.sha256(pg_catalog.convert_to(p_error,'UTF8'))
$$;
REVOKE ALL ON FUNCTION public.novu_rust_failure_digest(text) FROM PUBLIC;
DROP INDEX IF EXISTS public.novu_rust_queue_failure_ack_revision_idx;
CREATE UNIQUE INDEX IF NOT EXISTS novu_rust_queue_failure_ack_digest_idx ON public.novu_rust_queue_failure_acknowledgements(job_id,job_attempts,public.novu_rust_failure_digest(job_error),job_failure_revision);
DROP INDEX IF EXISTS public.novu_rust_slack_ack_revision_idx;
CREATE UNIQUE INDEX IF NOT EXISTS novu_rust_slack_ack_digest_idx ON public.novu_rust_slack_workflow_acknowledgements(job_id,job_attempts,job_status,public.novu_rust_failure_digest(job_error),job_attention_revision);

CREATE OR REPLACE FUNCTION public.novu_rust_ack_failed_queue(p_environment text,p_job uuid,p_operator text,p_reason text) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_attempts integer;v_error text;v_revision bigint;v_count integer;
BEGIN
 IF p_environment IS NULL OR length(btrim(p_environment))=0 OR p_job IS NULL OR p_operator IS NULL OR length(btrim(p_operator))=0 OR octet_length(p_operator)>128 OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN RAISE EXCEPTION 'invalid queue acknowledgement' USING ERRCODE='22023'; END IF;
 IF NOT public.novu_rust_lock_operator_grant(p_environment,'queue_ack') THEN RAISE EXCEPTION 'queue acknowledgement permission denied' USING ERRCODE='42501'; END IF;
 SELECT j.attempts,coalesce(j.last_error,''),j.failure_revision INTO v_attempts,v_error,v_revision FROM public.novu_queue_jobs j WHERE j.environment_id=p_environment AND j.id=p_job AND j.status='failed' FOR UPDATE OF j;
 IF NOT FOUND THEN RAISE EXCEPTION 'failed queue job not found' USING ERRCODE='22023'; END IF;
 INSERT INTO public.novu_rust_queue_failure_acknowledgements(id,environment_id,job_id,job_attempts,job_error,job_failure_revision,operator_name,reason) VALUES(gen_random_uuid(),p_environment,p_job,v_attempts,v_error,v_revision,p_operator,p_reason) ON CONFLICT(job_id,job_attempts,(public.novu_rust_failure_digest(job_error)),job_failure_revision) DO NOTHING;
 GET DIAGNOSTICS v_count=ROW_COUNT; RETURN v_count=1;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_ack_failed_queue(text,uuid,text,text) FROM PUBLIC;

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
      ON CONFLICT(job_id,job_attempts,job_status,(public.novu_rust_failure_digest(job_error)),job_attention_revision) DO NOTHING;
    GET DIAGNOSTICS v_inserted=ROW_COUNT;
    RETURN v_inserted=1;
END;
$$;
REVOKE ALL ON FUNCTION public.novu_rust_ack_slack_workflow(text,uuid,text,text) FROM PUBLIC;

