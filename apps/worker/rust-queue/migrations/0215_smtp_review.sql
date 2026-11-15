-- Each attempt update changes its revision, including changes reverted later.
ALTER TABLE public.novu_rust_smtp_attempts ADD COLUMN revision uuid NOT NULL DEFAULT gen_random_uuid();
CREATE FUNCTION public.novu_rust_smtp_attempt_revision() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN NEW.revision:=gen_random_uuid(); RETURN NEW; END; $$;
CREATE TRIGGER novu_rust_smtp_attempt_revision_trigger BEFORE UPDATE ON public.novu_rust_smtp_attempts
 FOR EACH ROW EXECUTE FUNCTION public.novu_rust_smtp_attempt_revision();

-- A review acknowledges only the exact failure snapshot; it does not change delivery state.
CREATE TABLE public.novu_rust_smtp_reviews (
 id uuid PRIMARY KEY, environment_id text NOT NULL, job_id uuid NOT NULL,
 attempt_revision uuid, attempt_state text, job_failure_revision bigint NOT NULL,
 job_attempts integer NOT NULL, job_error text NOT NULL, operator_name text NOT NULL,
 reason text NOT NULL, database_session_user text NOT NULL DEFAULT session_user,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
REVOKE ALL ON public.novu_rust_smtp_reviews FROM PUBLIC;
CREATE INDEX novu_rust_smtp_review_lookup_idx ON public.novu_rust_smtp_reviews(environment_id,job_id,job_failure_revision);

CREATE OR REPLACE FUNCTION public.novu_rust_smtp_health(p_env text)
RETURNS TABLE(unknown_attempts bigint,stranded_dispatches bigint,pre_dispatch_failures bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 PERFORM public.novu_rust_assert_alert_view(p_env);
 RETURN QUERY SELECT
   count(*) FILTER (WHERE a.state='unknown' AND NOT EXISTS(SELECT 1 FROM public.novu_rust_smtp_reviews r WHERE j.status='failed' AND r.environment_id=j.environment_id AND r.job_id=j.id AND r.attempt_revision=a.revision AND r.attempt_state=a.state AND r.job_failure_revision=j.failure_revision AND r.job_attempts=j.attempts AND r.job_error=coalesce(j.last_error,''))),
   count(*) FILTER (WHERE a.state='dispatching' AND (j.status<>'running' OR j.lease_until IS NULL OR j.lease_until<=statement_timestamp())),
   (SELECT count(*) FROM public.novu_queue_jobs x WHERE x.environment_id=p_env AND x.kind='experimental_smtp_email' AND x.status='failed'
      AND NOT EXISTS(SELECT 1 FROM public.novu_rust_smtp_attempts z WHERE z.job_id=x.id AND z.environment_id=x.environment_id)
      AND NOT EXISTS(SELECT 1 FROM public.novu_rust_smtp_reviews r WHERE r.environment_id=x.environment_id AND r.job_id=x.id AND r.attempt_revision IS NULL AND r.attempt_state IS NULL AND r.job_failure_revision=x.failure_revision AND r.job_attempts=x.attempts AND r.job_error=coalesce(x.last_error,'')))
 FROM public.novu_rust_smtp_attempts a JOIN public.novu_queue_jobs j ON j.id=a.job_id AND j.environment_id=a.environment_id
 WHERE a.environment_id=p_env AND j.kind='experimental_smtp_email';
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_smtp_health(text) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_smtp_attention(p_env text,p_after uuid)
RETURNS TABLE(job_id uuid,status text,attempt_state text,attempt_revision uuid,failure_revision bigint,attempts integer)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 PERFORM public.novu_rust_assert_alert_view(p_env);
 RETURN QUERY SELECT j.id,j.status,a.state,a.revision,j.failure_revision,j.attempts
 FROM public.novu_queue_jobs j LEFT JOIN public.novu_rust_smtp_attempts a ON a.job_id=j.id AND a.environment_id=j.environment_id
 WHERE j.environment_id=p_env AND j.kind='experimental_smtp_email' AND (p_after IS NULL OR j.id>p_after)
 AND ((a.state='unknown' AND j.status<>'failed')
   OR (a.state='dispatching' AND (j.status<>'running' OR j.lease_until IS NULL OR j.lease_until<=statement_timestamp()))
   OR (j.status='failed' AND (a.state='unknown' OR a.job_id IS NULL)
       AND NOT EXISTS(SELECT 1 FROM public.novu_rust_smtp_reviews r WHERE r.environment_id=j.environment_id AND r.job_id=j.id AND r.attempt_revision IS NOT DISTINCT FROM a.revision AND r.attempt_state IS NOT DISTINCT FROM a.state AND r.job_failure_revision=j.failure_revision AND r.job_attempts=j.attempts AND r.job_error=coalesce(j.last_error,''))))
 ORDER BY j.id LIMIT 101;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_smtp_attention(text,uuid) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_ack_smtp(p_env text,p_job uuid,p_attempt_revision uuid,p_failure_revision bigint,p_operator text,p_reason text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_job public.novu_queue_jobs%ROWTYPE; v_attempt public.novu_rust_smtp_attempts%ROWTYPE;
BEGIN
 IF p_env IS NULL OR length(btrim(p_env))=0 OR p_job IS NULL OR p_failure_revision IS NULL OR p_failure_revision<0 OR p_operator IS NULL OR length(btrim(p_operator))=0 OR octet_length(p_operator)>128 OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN
  RAISE EXCEPTION 'invalid SMTP review' USING ERRCODE='22023'; END IF;
 IF NOT public.novu_rust_lock_operator_grant(p_env,'queue_ack') THEN RAISE EXCEPTION 'SMTP review permission denied' USING ERRCODE='42501'; END IF;
 SELECT * INTO v_job FROM public.novu_queue_jobs WHERE environment_id=p_env AND id=p_job AND kind='experimental_smtp_email' FOR UPDATE;
 IF NOT FOUND OR v_job.status<>'failed' OR v_job.failure_revision<>p_failure_revision THEN RAISE EXCEPTION 'SMTP job changed or unavailable' USING ERRCODE='22023'; END IF;
 SELECT * INTO v_attempt FROM public.novu_rust_smtp_attempts WHERE environment_id=p_env AND job_id=p_job FOR UPDATE;
 IF FOUND THEN
  IF v_attempt.state<>'unknown' OR v_attempt.revision IS DISTINCT FROM p_attempt_revision THEN RAISE EXCEPTION 'SMTP attempt changed or unavailable' USING ERRCODE='22023'; END IF;
 ELSIF p_attempt_revision IS NOT NULL THEN RAISE EXCEPTION 'SMTP attempt missing' USING ERRCODE='22023'; END IF;
 IF EXISTS(SELECT 1 FROM public.novu_rust_smtp_reviews r WHERE r.environment_id=p_env AND r.job_id=p_job AND r.attempt_revision IS NOT DISTINCT FROM v_attempt.revision AND r.attempt_state IS NOT DISTINCT FROM v_attempt.state AND r.job_failure_revision=v_job.failure_revision AND r.job_attempts=v_job.attempts AND r.job_error=coalesce(v_job.last_error,'')) THEN RETURN false; END IF;
 INSERT INTO public.novu_rust_smtp_reviews(id,environment_id,job_id,attempt_revision,attempt_state,job_failure_revision,job_attempts,job_error,operator_name,reason) VALUES(gen_random_uuid(),p_env,p_job,v_attempt.revision,v_attempt.state,v_job.failure_revision,v_job.attempts,coalesce(v_job.last_error,''),p_operator,p_reason);
 RETURN true;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_ack_smtp(text,uuid,uuid,bigint,text,text) FROM PUBLIC;
