-- Newly inserted authenticated webhook events invalidate matching stored projections.
CREATE OR REPLACE FUNCTION public.novu_rust_invalidate_resend_projection()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  -- Always acquire job locks, including when already unknown. A projector may
  -- hold the lock while writing a result from a snapshot predating this event.
  UPDATE public.novu_queue_jobs j SET resend_delivery_result='unknown'
  WHERE j.kind='experimental_resend_email' AND EXISTS (
    SELECT 1 FROM inserted_resend_events e JOIN public.novu_rust_resend_attempts a
      ON a.environment_id=e.environment_id AND a.provider_message_id=e.email_id
    WHERE a.job_id=j.id AND a.environment_id=j.environment_id);
  UPDATE public.novu_rust_email_messages m SET resend_delivery_result='unknown'
  WHERE m.resend_delivery_result IS DISTINCT FROM 'unknown' AND EXISTS (
    SELECT 1 FROM inserted_resend_events e JOIN public.novu_rust_resend_attempts a
      ON a.environment_id=e.environment_id AND a.provider_message_id=e.email_id
    WHERE a.job_id=m.email_job_id AND a.environment_id=m.environment_id);
  RETURN NULL;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_invalidate_resend_projection() FROM PUBLIC;
DROP TRIGGER IF EXISTS novu_rust_resend_projection_invalidate ON public.novu_rust_resend_delivery_events;
CREATE TRIGGER novu_rust_resend_projection_invalidate AFTER INSERT ON public.novu_rust_resend_delivery_events
  REFERENCING NEW TABLE AS inserted_resend_events FOR EACH STATEMENT EXECUTE FUNCTION public.novu_rust_invalidate_resend_projection();
