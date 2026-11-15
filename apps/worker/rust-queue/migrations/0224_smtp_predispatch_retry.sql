-- Permit an explicitly authorized retry when the locked failed job has no attempt.
ALTER TABLE public.novu_rust_smtp_resolutions ALTER COLUMN attempt_revision DROP NOT NULL;
CREATE OR REPLACE FUNCTION public.novu_rust_resolve_smtp(p_env text,p_request uuid,p_job uuid,p_attempt_revision uuid,p_failure_revision bigint,p_decision text,p_reason text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_actor oid; v_existing public.novu_rust_smtp_resolutions%ROWTYPE; v_job public.novu_queue_jobs%ROWTYPE; v_attempt public.novu_rust_smtp_attempts%ROWTYPE; v_inserted boolean;
BEGIN
 IF p_env IS NULL OR length(btrim(p_env))=0 OR p_request IS NULL OR p_job IS NULL OR (p_attempt_revision IS NULL AND p_decision IS DISTINCT FROM 'retry') OR p_failure_revision IS NULL OR p_failure_revision<0 OR p_decision IS NULL OR p_decision NOT IN ('accepted','retry') OR p_reason IS NULL OR octet_length(btrim(p_reason)) NOT BETWEEN 1 AND 512 THEN
  RAISE EXCEPTION 'invalid SMTP resolution' USING ERRCODE='22023'; END IF;
 SELECT r.oid INTO v_actor FROM public.novu_rust_smtp_resolvers g JOIN pg_catalog.pg_roles r ON r.oid=g.principal_oid AND r.rolname=g.database_principal AND r.rolcanlogin
 WHERE g.environment_id=p_env AND g.database_principal=session_user AND CASE WHEN p_decision='accepted' THEN g.can_accept ELSE g.can_retry END FOR SHARE OF g;
 IF NOT FOUND THEN RAISE EXCEPTION 'SMTP resolution permission denied' USING ERRCODE='42501'; END IF;
 INSERT INTO public.novu_rust_smtp_resolutions(environment_id,actor,actor_oid,request_id,job_id,attempt_revision,failure_revision,decision,reason)
 VALUES(p_env,session_user,v_actor,p_request,p_job,p_attempt_revision,p_failure_revision,p_decision,p_reason) ON CONFLICT DO NOTHING;
 v_inserted:=FOUND;
 IF NOT v_inserted THEN
  SELECT * INTO v_existing FROM public.novu_rust_smtp_resolutions WHERE environment_id=p_env AND actor_oid=v_actor AND request_id=p_request;
  IF v_existing.actor<>session_user OR v_existing.job_id<>p_job OR v_existing.attempt_revision IS DISTINCT FROM p_attempt_revision OR v_existing.failure_revision<>p_failure_revision OR v_existing.decision<>p_decision OR v_existing.reason<>p_reason THEN
   RAISE EXCEPTION 'SMTP resolution request conflict' USING ERRCODE='22023'; END IF;
  RETURN false;
 END IF;
 SELECT * INTO v_job FROM public.novu_queue_jobs WHERE environment_id=p_env AND id=p_job AND kind='experimental_smtp_email' FOR UPDATE;
 IF NOT FOUND OR v_job.status<>'failed' OR v_job.failure_revision<>p_failure_revision THEN RAISE EXCEPTION 'SMTP job changed or unavailable' USING ERRCODE='22023'; END IF;
 SELECT * INTO v_attempt FROM public.novu_rust_smtp_attempts WHERE environment_id=p_env AND job_id=p_job FOR UPDATE;
 IF (p_attempt_revision IS NULL AND FOUND) OR (p_attempt_revision IS NOT NULL AND (NOT FOUND OR v_attempt.state<>'unknown' OR v_attempt.revision IS DISTINCT FROM p_attempt_revision)) THEN RAISE EXCEPTION 'SMTP attempt changed or unavailable' USING ERRCODE='22023'; END IF;
 IF p_decision='accepted' THEN
  UPDATE public.novu_rust_smtp_attempts SET state='accepted',updated_at=clock_timestamp() WHERE job_id=p_job AND environment_id=p_env;
  UPDATE public.novu_rust_email_messages SET delivery_state='accepted',status='sent',error_id=NULL,error_text=NULL WHERE environment_id=p_env AND email_job_id=p_job;
  UPDATE public.novu_queue_jobs SET status='completed',last_error=NULL,lease_token=NULL,lease_until=NULL WHERE environment_id=p_env AND id=p_job;
 ELSE
  DELETE FROM public.novu_rust_smtp_attempts WHERE job_id=p_job AND environment_id=p_env;
  UPDATE public.novu_rust_email_messages SET delivery_state='not_attempted',status='warning',error_id=NULL,error_text=NULL WHERE environment_id=p_env AND email_job_id=p_job;
  UPDATE public.novu_queue_jobs SET status='pending',available_at=clock_timestamp(),last_error=NULL,lease_token=NULL,lease_until=NULL WHERE environment_id=p_env AND id=p_job;
 END IF;
 RETURN true;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resolve_smtp(text,uuid,uuid,uuid,bigint,text,text) FROM PUBLIC;
