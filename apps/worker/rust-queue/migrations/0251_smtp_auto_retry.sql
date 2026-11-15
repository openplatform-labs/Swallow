-- SMTP has no idempotency key. This optional policy intentionally chooses
-- at-least-once recovery and can duplicate externally accepted messages.
LOCK TABLE public.novu_rust_environments IN SHARE ROW EXCLUSIVE MODE;
CREATE TABLE public.novu_rust_smtp_auto_retry_policy (
 environment_id text PRIMARY KEY REFERENCES public.novu_rust_environments(id) ON DELETE CASCADE,
 enabled boolean NOT NULL DEFAULT false, max_retries integer NOT NULL DEFAULT 1 CHECK(max_retries BETWEEN 1 AND 10),
 delay_seconds integer NOT NULL DEFAULT 60 CHECK(delay_seconds BETWEEN 10 AND 86400), eligible_since timestamptz,
 CHECK(NOT enabled OR eligible_since IS NOT NULL)
);
CREATE TABLE public.novu_rust_smtp_auto_retry_workers (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),database_principal name NOT NULL,principal_oid oid NOT NULL,
 PRIMARY KEY(environment_id,database_principal)
);
CREATE TABLE public.novu_rust_smtp_auto_retries (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,environment_id text NOT NULL,job_id uuid NOT NULL REFERENCES public.novu_queue_jobs(id),
 attempt_revision uuid NOT NULL,failure_revision bigint NOT NULL,database_principal name NOT NULL,principal_oid oid NOT NULL,
 policy_max integer NOT NULL,policy_delay_seconds integer NOT NULL,recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(environment_id,job_id,attempt_revision)
);
CREATE INDEX novu_rust_smtp_auto_retry_job ON public.novu_rust_smtp_auto_retries(environment_id,job_id);
REVOKE ALL ON public.novu_rust_smtp_auto_retry_policy,public.novu_rust_smtp_auto_retry_workers,public.novu_rust_smtp_auto_retries FROM PUBLIC;
INSERT INTO public.novu_rust_smtp_auto_retry_policy(environment_id) SELECT id FROM public.novu_rust_environments;
CREATE FUNCTION public.novu_rust_smtp_auto_retry_init() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN INSERT INTO public.novu_rust_smtp_auto_retry_policy(environment_id) VALUES(NEW.id); RETURN NEW; END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_smtp_auto_retry_init() FROM PUBLIC;
CREATE TRIGGER novu_rust_smtp_auto_retry_init_trigger AFTER INSERT ON public.novu_rust_environments FOR EACH ROW EXECUTE FUNCTION public.novu_rust_smtp_auto_retry_init();

CREATE FUNCTION public.novu_rust_smtp_auto_retry_batch(p_env text,p_limit integer) RETURNS integer
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_policy public.novu_rust_smtp_auto_retry_policy%ROWTYPE; v_actor oid; v_job record; v_attempt public.novu_rust_smtp_attempts%ROWTYPE; v_count integer:=0;
BEGIN
 IF p_env IS NULL OR length(btrim(p_env))=0 OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 25 THEN RAISE EXCEPTION 'invalid SMTP auto retry request' USING ERRCODE='22023'; END IF;
 SELECT r.oid INTO v_actor FROM public.novu_rust_smtp_auto_retry_workers w JOIN pg_catalog.pg_roles r ON r.oid=w.principal_oid AND r.rolname=w.database_principal AND r.rolcanlogin
 WHERE w.environment_id=p_env AND w.database_principal=session_user FOR SHARE OF w;
 IF NOT FOUND THEN RAISE EXCEPTION 'SMTP auto retry denied' USING ERRCODE='42501'; END IF;
 SELECT * INTO v_policy FROM public.novu_rust_smtp_auto_retry_policy WHERE environment_id=p_env FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION 'SMTP auto retry policy missing' USING ERRCODE='55000'; END IF;
 IF NOT v_policy.enabled THEN RETURN 0; END IF;
 FOR v_job IN SELECT j.id,j.failure_revision,j.attempts,coalesce(j.last_error,'') AS job_error FROM public.novu_queue_jobs j JOIN public.novu_rust_smtp_attempts a ON a.job_id=j.id AND a.environment_id=j.environment_id
  WHERE j.environment_id=p_env AND j.kind='experimental_smtp_email' AND j.status='failed' AND a.state='unknown'
   AND a.updated_at>=v_policy.eligible_since AND a.updated_at<=clock_timestamp()-v_policy.delay_seconds*interval '1 second'
   AND j.attachment_scrubbed_at IS NULL AND j.alternative_scrubbed_at IS NULL AND j.tracking_scrubbed_at IS NULL
   AND NOT EXISTS(SELECT 1 FROM public.novu_rust_smtp_reviews r WHERE r.environment_id=j.environment_id AND r.job_id=j.id AND r.attempt_revision=a.revision AND r.attempt_state=a.state AND r.job_failure_revision=j.failure_revision AND r.job_attempts=j.attempts AND r.job_error=coalesce(j.last_error,''))
   AND (SELECT count(*) FROM public.novu_rust_smtp_auto_retries h WHERE h.environment_id=p_env AND h.job_id=j.id)<v_policy.max_retries
  ORDER BY a.updated_at,j.id FOR UPDATE OF j SKIP LOCKED LIMIT p_limit LOOP
  SELECT * INTO v_attempt FROM public.novu_rust_smtp_attempts WHERE environment_id=p_env AND job_id=v_job.id FOR UPDATE;
  IF NOT FOUND OR v_attempt.state<>'unknown' OR v_attempt.updated_at<v_policy.eligible_since OR v_attempt.updated_at>clock_timestamp()-v_policy.delay_seconds*interval '1 second' THEN CONTINUE; END IF;
  -- Re-read after taking the job lock: an operator may have reviewed this
  -- failure while the candidate query was waiting on another transaction.
  IF EXISTS(SELECT 1 FROM public.novu_rust_smtp_reviews r WHERE r.environment_id=p_env AND r.job_id=v_job.id AND r.attempt_revision=v_attempt.revision AND r.attempt_state=v_attempt.state AND r.job_failure_revision=v_job.failure_revision AND r.job_attempts=v_job.attempts AND r.job_error=v_job.job_error) THEN CONTINUE; END IF;
  INSERT INTO public.novu_rust_smtp_auto_retries(environment_id,job_id,attempt_revision,failure_revision,database_principal,principal_oid,policy_max,policy_delay_seconds)
   VALUES(p_env,v_job.id,v_attempt.revision,v_job.failure_revision,session_user,v_actor,v_policy.max_retries,v_policy.delay_seconds);
  DELETE FROM public.novu_rust_smtp_attempts WHERE environment_id=p_env AND job_id=v_job.id;
  UPDATE public.novu_rust_email_messages SET delivery_state='not_attempted',status='warning',error_id=NULL,error_text=NULL WHERE environment_id=p_env AND email_job_id=v_job.id;
  UPDATE public.novu_queue_jobs SET status='pending',available_at=clock_timestamp(),last_error=NULL,lease_token=NULL,lease_until=NULL WHERE environment_id=p_env AND id=v_job.id;
  v_count:=v_count+1;
 END LOOP;
 RETURN v_count;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_smtp_auto_retry_batch(text,integer) FROM PUBLIC;
