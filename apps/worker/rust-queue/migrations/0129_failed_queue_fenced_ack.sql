-- Versioned review prevents an acknowledgement of a later failure than the one inspected.
CREATE OR REPLACE FUNCTION public.novu_rust_failed_queue_list_v2(p_environment text,p_after uuid)
RETURNS TABLE(job_id uuid,kind text,attempts integer,created_at text,revision text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 PERFORM public.novu_rust_assert_alert_view(p_environment);
 RETURN QUERY SELECT j.id,j.kind,j.attempts,j.created_at::text,j.failure_revision::text
 FROM public.novu_queue_jobs j WHERE j.environment_id=p_environment AND j.status='failed'
 AND (p_after IS NULL OR j.id>p_after)
 AND NOT EXISTS(SELECT 1 FROM public.novu_rust_queue_failure_acknowledgements a WHERE a.environment_id=j.environment_id AND a.job_id=j.id AND a.job_attempts=j.attempts AND a.job_error=coalesce(j.last_error,'') AND a.job_failure_revision=j.failure_revision)
 ORDER BY j.id LIMIT 100;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_failed_queue_list_v2(text,uuid) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_ack_failed_queue_v2(p_environment text,p_job uuid,p_revision bigint,p_operator text,p_reason text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_attempts integer; v_error text; v_revision bigint; v_count integer;
BEGIN
 IF p_environment IS NULL OR length(btrim(p_environment))=0 OR p_job IS NULL OR p_revision IS NULL OR p_revision<0 OR p_operator IS NULL OR length(btrim(p_operator))=0 OR octet_length(p_operator)>128 OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN RAISE EXCEPTION 'invalid queue acknowledgement' USING ERRCODE='22023'; END IF;
 IF NOT public.novu_rust_lock_operator_grant(p_environment,'queue_ack') THEN RAISE EXCEPTION 'queue acknowledgement permission denied' USING ERRCODE='42501'; END IF;
 SELECT j.attempts,coalesce(j.last_error,''),j.failure_revision INTO v_attempts,v_error,v_revision FROM public.novu_queue_jobs j WHERE j.environment_id=p_environment AND j.id=p_job AND j.status='failed' FOR UPDATE OF j;
 IF NOT FOUND THEN RAISE EXCEPTION 'failed queue job not found' USING ERRCODE='22023'; END IF;
 IF v_revision<>p_revision THEN RAISE EXCEPTION 'failure changed; review again' USING ERRCODE='40001'; END IF;
 INSERT INTO public.novu_rust_queue_failure_acknowledgements(id,environment_id,job_id,job_attempts,job_error,job_failure_revision,operator_name,reason) VALUES(gen_random_uuid(),p_environment,p_job,v_attempts,v_error,v_revision,p_operator,p_reason) ON CONFLICT(job_id,job_attempts,(public.novu_rust_failure_digest(job_error)),job_failure_revision) DO NOTHING;
 GET DIAGNOSTICS v_count=ROW_COUNT; RETURN v_count=1;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_ack_failed_queue_v2(text,uuid,bigint,text,text) FROM PUBLIC;
