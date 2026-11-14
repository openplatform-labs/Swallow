-- Restrict acknowledgements to a single audited operation with no direct table grants.
CREATE OR REPLACE FUNCTION public.novu_rust_ack_slack_workflow(
    p_environment text,p_job uuid,p_operator text,p_reason text
) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_attempts integer; v_status text; v_error text; v_inserted integer;
BEGIN
    IF p_environment IS NULL OR length(btrim(p_environment))=0 OR p_job IS NULL
       OR p_operator IS NULL OR length(btrim(p_operator))=0 OR octet_length(p_operator)>128
       OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN
        RAISE EXCEPTION 'invalid Slack acknowledgement' USING ERRCODE='22023';
    END IF;
    IF NOT public.novu_rust_lock_operator_grant(p_environment,'slack_ack') THEN
        RAISE EXCEPTION 'Slack acknowledgement permission denied' USING ERRCODE='42501';
    END IF;
    SELECT j.attempts,j.status,j.last_error INTO v_attempts,v_status,v_error
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
      (id,environment_id,job_id,job_attempts,job_status,job_error,operator_name,reason)
      VALUES (gen_random_uuid(),p_environment,p_job,v_attempts,v_status,v_error,p_operator,p_reason)
      ON CONFLICT(job_id,job_attempts,job_status,job_error) DO NOTHING;
    GET DIAGNOSTICS v_inserted=ROW_COUNT;
    RETURN v_inserted=1;
END;
$$;
REVOKE ALL ON FUNCTION public.novu_rust_ack_slack_workflow(text,uuid,text,text) FROM PUBLIC;
