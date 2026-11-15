-- Optional operator decision; existing immediate Slack retry remains available.
ALTER TABLE public.novu_rust_slack_resolutions ADD COLUMN retry_not_before timestamptz;
CREATE FUNCTION public.novu_rust_resolve_slack_after(p_env text,p_job uuid,p_attempt uuid,p_operator text,p_reason text)
RETURNS timestamptz LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_until timestamptz; v_scheduled timestamptz;
BEGIN
 IF p_env IS NULL OR length(btrim(p_env))=0 OR p_job IS NULL OR p_attempt IS NULL OR p_operator IS NULL OR length(btrim(p_operator))=0 OR octet_length(p_operator)>128 OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN
  RAISE EXCEPTION 'invalid Slack resolution' USING ERRCODE='22023';
 END IF;
 IF NOT public.novu_rust_lock_operator_grant(p_env,'slack_retry') THEN RAISE EXCEPTION 'Slack resolution permission denied' USING ERRCODE='42501'; END IF;
 SELECT a.retry_not_before INTO v_until FROM public.novu_queue_jobs j JOIN public.novu_rust_slack_attempts a ON a.job_id=j.id AND a.environment_id=j.environment_id
 WHERE j.id=p_job AND j.environment_id=p_env AND j.kind IN ('experimental_slack_chat','experimental_slack_webhook','experimental_slack_workflow_delivery') AND j.status='failed'
 AND a.attempt_id=p_attempt AND a.state='rejected' AND a.http_status=429 AND a.retry_after_seconds IS NOT NULL AND a.retry_not_before IS NOT NULL FOR UPDATE OF j,a;
 IF NOT FOUND THEN RAISE EXCEPTION 'current Slack 429 with retry guidance required' USING ERRCODE='22023'; END IF;
 INSERT INTO public.novu_rust_slack_resolutions(id,environment_id,job_id,attempt_id,previous_state,decision,operator_name,reason,database_user,database_session_user,retry_not_before)
 VALUES(gen_random_uuid(),p_env,p_job,p_attempt,'rejected','retry',p_operator,p_reason,session_user,session_user,v_until);
 DELETE FROM public.novu_rust_slack_attempts WHERE job_id=p_job AND environment_id=p_env AND attempt_id=p_attempt;
 UPDATE public.novu_queue_jobs SET status='pending',available_at=greatest(v_until,clock_timestamp()),last_error=NULL WHERE id=p_job AND environment_id=p_env AND status='failed' RETURNING available_at INTO v_scheduled;
 IF NOT FOUND THEN RAISE EXCEPTION 'retry job not pending' USING ERRCODE='55000'; END IF;
 RETURN v_scheduled;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resolve_slack_after(text,uuid,uuid,text,text) FROM PUBLIC;
