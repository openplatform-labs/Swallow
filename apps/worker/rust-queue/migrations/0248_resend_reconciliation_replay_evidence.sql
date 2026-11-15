-- Preserve the attempt snapshot before reconciliation can replace the attempt.
-- Historical decisions cannot be backfilled reliably.
ALTER TABLE public.novu_rust_delivery_reconciliations
 ADD COLUMN resend_attempt_id uuid,
 ADD COLUMN resend_replay_reserved boolean,
 ADD CONSTRAINT novu_rust_resend_replay_evidence_pair CHECK ((resend_attempt_id IS NULL) = (resend_replay_reserved IS NULL));

CREATE FUNCTION public.novu_rust_capture_resend_replay_evidence()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 NEW.resend_attempt_id := NULL;
 NEW.resend_replay_reserved := NULL;
 IF NEW.target_index IS NULL AND NEW.decision IN ('retry','accepted') THEN
  SELECT a.attempt_id, a.same_request_replays=1
   INTO NEW.resend_attempt_id, NEW.resend_replay_reserved
   FROM public.novu_queue_jobs j JOIN public.novu_rust_resend_attempts a
    ON a.environment_id=j.environment_id AND a.job_id=j.id
   WHERE j.environment_id=NEW.environment_id AND j.id=NEW.job_id
    AND j.kind='experimental_resend_email';
 END IF;
 RETURN NEW;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_capture_resend_replay_evidence() FROM PUBLIC;
CREATE TRIGGER novu_rust_resend_replay_evidence_capture
 BEFORE INSERT ON public.novu_rust_delivery_reconciliations
 FOR EACH ROW EXECUTE FUNCTION public.novu_rust_capture_resend_replay_evidence();
