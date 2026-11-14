-- A conservative guard for already auto-correlated attempts, including
-- older projectors that do not understand the correlation audit.
CREATE INDEX IF NOT EXISTS novu_rust_auto_resend_correlation_lookup_idx
  ON public.novu_rust_resend_auto_correlations(environment_id,correlation_id,job_id);
CREATE INDEX IF NOT EXISTS novu_rust_auto_resend_provider_lookup_idx
  ON public.novu_rust_resend_auto_correlations(environment_id,provider_message_id,job_id);
CREATE OR REPLACE FUNCTION public.novu_rust_resend_auto_evidence_conflicts(p_environment text,p_job uuid)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  RETURN EXISTS(SELECT 1 FROM public.novu_rust_resend_auto_correlations c
    WHERE c.environment_id=p_environment AND c.job_id=p_job AND (
      NOT EXISTS(SELECT 1 FROM public.novu_rust_resend_delivery_events e
        JOIN public.novu_rust_resend_recipients r ON r.environment_id=e.environment_id
          AND r.job_id=c.job_id AND r.recipient_hash=e.recipient_hash
        WHERE e.environment_id=c.environment_id AND e.correlation_id=c.correlation_id
          AND e.email_id=c.provider_message_id)
      OR EXISTS(SELECT 1 FROM public.novu_rust_resend_delivery_events e
        WHERE e.environment_id=c.environment_id AND
          ((e.correlation_id=c.correlation_id AND e.email_id<>c.provider_message_id)
            OR (e.email_id=c.provider_message_id AND e.correlation_id IS DISTINCT FROM c.correlation_id)))
      OR EXISTS(SELECT 1 FROM public.novu_rust_resend_attempts a
        WHERE a.environment_id=c.environment_id AND a.provider_message_id=c.provider_message_id AND a.job_id<>c.job_id)
      OR EXISTS(SELECT 1 FROM public.novu_rust_delivery_reconciliations r
        WHERE r.environment_id=c.environment_id AND r.retry_provider_message_id=c.provider_message_id)));
END;
$$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_auto_evidence_conflicts(text,uuid) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_guard_auto_resend_job()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  IF NEW.resend_delivery_result='delivered' AND
     public.novu_rust_resend_auto_evidence_conflicts(NEW.environment_id,NEW.id) THEN
    NEW.resend_delivery_result := 'unknown';
  END IF;
  RETURN NEW;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_guard_auto_resend_job() FROM PUBLIC;
DROP TRIGGER IF EXISTS novu_rust_guard_auto_resend_job ON public.novu_queue_jobs;
CREATE TRIGGER novu_rust_guard_auto_resend_job BEFORE UPDATE OF resend_delivery_result
  ON public.novu_queue_jobs FOR EACH ROW EXECUTE FUNCTION public.novu_rust_guard_auto_resend_job();

CREATE OR REPLACE FUNCTION public.novu_rust_guard_auto_resend_message()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  IF NEW.resend_delivery_result='delivered' AND NEW.email_job_id IS NOT NULL AND
     public.novu_rust_resend_auto_evidence_conflicts(NEW.environment_id,NEW.email_job_id) THEN
    NEW.resend_delivery_result := 'unknown';
  END IF;
  RETURN NEW;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_guard_auto_resend_message() FROM PUBLIC;
DROP TRIGGER IF EXISTS novu_rust_guard_auto_resend_message ON public.novu_rust_email_messages;
CREATE TRIGGER novu_rust_guard_auto_resend_message BEFORE UPDATE OF resend_delivery_result
  ON public.novu_rust_email_messages FOR EACH ROW EXECUTE FUNCTION public.novu_rust_guard_auto_resend_message();

-- New events can conflict by tag even when their provider ID differs from the
-- accepted one. Lock affected jobs in order and invalidate in the same commit.
CREATE OR REPLACE FUNCTION public.novu_rust_invalidate_auto_resend_insert()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  PERFORM j.id FROM public.novu_queue_jobs j WHERE EXISTS (
    SELECT 1 FROM public.novu_rust_resend_auto_correlations c JOIN inserted_auto_resend_events e
      ON e.environment_id=c.environment_id AND (e.email_id=c.provider_message_id OR e.correlation_id=c.correlation_id)
    WHERE c.job_id=j.id AND c.environment_id=j.environment_id) ORDER BY j.id FOR UPDATE;
  UPDATE public.novu_queue_jobs j SET resend_delivery_result='unknown' WHERE EXISTS (
    SELECT 1 FROM public.novu_rust_resend_auto_correlations c JOIN inserted_auto_resend_events e
      ON e.environment_id=c.environment_id AND (e.email_id=c.provider_message_id OR e.correlation_id=c.correlation_id)
    WHERE c.job_id=j.id AND c.environment_id=j.environment_id);
  UPDATE public.novu_rust_email_messages m SET resend_delivery_result='unknown' WHERE EXISTS (
    SELECT 1 FROM public.novu_rust_resend_auto_correlations c JOIN inserted_auto_resend_events e
      ON e.environment_id=c.environment_id AND (e.email_id=c.provider_message_id OR e.correlation_id=c.correlation_id)
    WHERE c.job_id=m.email_job_id AND c.environment_id=m.environment_id);
  RETURN NULL;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_invalidate_auto_resend_insert() FROM PUBLIC;
DROP TRIGGER IF EXISTS novu_rust_auto_resend_insert ON public.novu_rust_resend_delivery_events;
CREATE TRIGGER novu_rust_auto_resend_insert AFTER INSERT ON public.novu_rust_resend_delivery_events
  REFERENCING NEW TABLE AS inserted_auto_resend_events FOR EACH STATEMENT EXECUTE FUNCTION public.novu_rust_invalidate_auto_resend_insert();

CREATE OR REPLACE FUNCTION public.novu_rust_invalidate_auto_resend_delete()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  PERFORM j.id FROM public.novu_queue_jobs j WHERE EXISTS (
    SELECT 1 FROM public.novu_rust_resend_auto_correlations c JOIN deleted_auto_resend_events e
      ON e.environment_id=c.environment_id AND (e.email_id=c.provider_message_id OR e.correlation_id=c.correlation_id)
    WHERE c.job_id=j.id AND c.environment_id=j.environment_id) ORDER BY j.id FOR UPDATE;
  UPDATE public.novu_queue_jobs j SET resend_delivery_result='unknown' WHERE EXISTS (
    SELECT 1 FROM public.novu_rust_resend_auto_correlations c JOIN deleted_auto_resend_events e
      ON e.environment_id=c.environment_id AND (e.email_id=c.provider_message_id OR e.correlation_id=c.correlation_id)
    WHERE c.job_id=j.id AND c.environment_id=j.environment_id);
  UPDATE public.novu_rust_email_messages m SET resend_delivery_result='unknown' WHERE EXISTS (
    SELECT 1 FROM public.novu_rust_resend_auto_correlations c JOIN deleted_auto_resend_events e
      ON e.environment_id=c.environment_id AND (e.email_id=c.provider_message_id OR e.correlation_id=c.correlation_id)
    WHERE c.job_id=m.email_job_id AND c.environment_id=m.environment_id);
  RETURN NULL;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_invalidate_auto_resend_delete() FROM PUBLIC;
DROP TRIGGER IF EXISTS novu_rust_auto_resend_delete ON public.novu_rust_resend_delivery_events;
CREATE TRIGGER novu_rust_auto_resend_delete AFTER DELETE ON public.novu_rust_resend_delivery_events
  REFERENCING OLD TABLE AS deleted_auto_resend_events FOR EACH STATEMENT EXECUTE FUNCTION public.novu_rust_invalidate_auto_resend_delete();
