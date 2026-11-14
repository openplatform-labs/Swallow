-- Record each new failure transition independently of its diagnostic and attempt count.
ALTER TABLE public.novu_queue_jobs ADD COLUMN IF NOT EXISTS failure_revision bigint NOT NULL DEFAULT 0;
ALTER TABLE public.novu_rust_queue_failure_acknowledgements ADD COLUMN IF NOT EXISTS job_failure_revision bigint NOT NULL DEFAULT 0;
CREATE OR REPLACE FUNCTION public.novu_rust_queue_failure_revision() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF NEW.status='failed' AND (OLD.status IS DISTINCT FROM NEW.status OR OLD.attempts IS DISTINCT FROM NEW.attempts OR OLD.last_error IS DISTINCT FROM NEW.last_error) THEN
   NEW.failure_revision:=OLD.failure_revision+1;
 END IF;
 RETURN NEW;
END; $$;
DROP TRIGGER IF EXISTS novu_rust_queue_failure_revision_trigger ON public.novu_queue_jobs;
CREATE TRIGGER novu_rust_queue_failure_revision_trigger BEFORE UPDATE OF status,attempts,last_error ON public.novu_queue_jobs
 FOR EACH ROW EXECUTE FUNCTION public.novu_rust_queue_failure_revision();
DO $$
DECLARE v_name text;
BEGIN
 SELECT c.conname INTO v_name FROM pg_catalog.pg_constraint c WHERE c.conrelid='public.novu_rust_queue_failure_acknowledgements'::regclass AND c.contype='u' AND pg_catalog.pg_get_constraintdef(c.oid)='UNIQUE (job_id, job_attempts, job_error)';
 IF v_name IS NOT NULL THEN EXECUTE pg_catalog.format('ALTER TABLE public.novu_rust_queue_failure_acknowledgements DROP CONSTRAINT %I',v_name); END IF;
END; $$;
CREATE UNIQUE INDEX IF NOT EXISTS novu_rust_queue_failure_ack_revision_idx ON public.novu_rust_queue_failure_acknowledgements(job_id,job_attempts,job_error,job_failure_revision);

CREATE OR REPLACE FUNCTION public.novu_rust_failed_queue_count(p_environment text) RETURNS bigint
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE n bigint;
BEGIN
 PERFORM public.novu_rust_assert_alert_view(p_environment);
 SELECT count(*) INTO n FROM public.novu_queue_jobs j WHERE j.environment_id=p_environment AND j.status='failed'
 AND NOT EXISTS(SELECT 1 FROM public.novu_rust_queue_failure_acknowledgements a WHERE a.environment_id=j.environment_id AND a.job_id=j.id AND a.job_attempts=j.attempts AND a.job_error=coalesce(j.last_error,'') AND a.job_failure_revision=j.failure_revision);
 RETURN n;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_failed_queue_count(text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_failed_queue_list(p_environment text,p_after uuid)
RETURNS TABLE(job_id uuid,kind text,attempts integer,created_at text) LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 PERFORM public.novu_rust_assert_alert_view(p_environment);
 RETURN QUERY SELECT j.id,j.kind,j.attempts,j.created_at::text FROM public.novu_queue_jobs j WHERE j.environment_id=p_environment AND j.status='failed' AND (p_after IS NULL OR j.id>p_after)
 AND NOT EXISTS(SELECT 1 FROM public.novu_rust_queue_failure_acknowledgements a WHERE a.environment_id=j.environment_id AND a.job_id=j.id AND a.job_attempts=j.attempts AND a.job_error=coalesce(j.last_error,'') AND a.job_failure_revision=j.failure_revision) ORDER BY j.id LIMIT 100;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_failed_queue_list(text,uuid) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_ack_failed_queue(p_environment text,p_job uuid,p_operator text,p_reason text) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_attempts integer;v_error text;v_revision bigint;v_count integer;
BEGIN
 IF p_environment IS NULL OR length(btrim(p_environment))=0 OR p_job IS NULL OR p_operator IS NULL OR length(btrim(p_operator))=0 OR octet_length(p_operator)>128 OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN RAISE EXCEPTION 'invalid queue acknowledgement' USING ERRCODE='22023'; END IF;
 IF NOT public.novu_rust_lock_operator_grant(p_environment,'queue_ack') THEN RAISE EXCEPTION 'queue acknowledgement permission denied' USING ERRCODE='42501'; END IF;
 SELECT j.attempts,coalesce(j.last_error,''),j.failure_revision INTO v_attempts,v_error,v_revision FROM public.novu_queue_jobs j WHERE j.environment_id=p_environment AND j.id=p_job AND j.status='failed' FOR UPDATE OF j;
 IF NOT FOUND THEN RAISE EXCEPTION 'failed queue job not found' USING ERRCODE='22023'; END IF;
 INSERT INTO public.novu_rust_queue_failure_acknowledgements(id,environment_id,job_id,job_attempts,job_error,job_failure_revision,operator_name,reason) VALUES(gen_random_uuid(),p_environment,p_job,v_attempts,v_error,v_revision,p_operator,p_reason) ON CONFLICT(job_id,job_attempts,job_error,job_failure_revision) DO NOTHING;
 GET DIAGNOSTICS v_count=ROW_COUNT; RETURN v_count=1;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_ack_failed_queue(text,uuid,text,text) FROM PUBLIC;
