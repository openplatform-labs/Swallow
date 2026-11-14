-- Invalidate stored results atomically with new, verified current-dispatch evidence.
CREATE OR REPLACE FUNCTION public.novu_rust_invalidate_sendgrid_projection()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  UPDATE public.novu_queue_jobs j SET sendgrid_delivery_result='unknown'
  WHERE j.kind='experimental_sendgrid_email' AND j.sendgrid_delivery_result IS DISTINCT FROM 'unknown'
    AND EXISTS (SELECT 1 FROM inserted_events e
      JOIN public.novu_rust_email_messages m ON m.environment_id=e.environment_id AND m.internal_id=e.message_internal_id
      JOIN public.novu_rust_sendgrid_attempts a ON a.environment_id=m.environment_id AND a.job_id=m.email_job_id AND a.dispatch_id=e.dispatch_id
      WHERE e.dispatch_proof_verified AND j.id=a.job_id AND j.environment_id=a.environment_id);
  UPDATE public.novu_rust_email_messages m SET sendgrid_delivery_result='unknown'
  WHERE m.sendgrid_delivery_result IS DISTINCT FROM 'unknown'
    AND EXISTS (SELECT 1 FROM inserted_events e
      JOIN public.novu_rust_email_messages linked ON linked.environment_id=e.environment_id AND linked.internal_id=e.message_internal_id
      JOIN public.novu_rust_sendgrid_attempts a ON a.environment_id=linked.environment_id AND a.job_id=linked.email_job_id AND a.dispatch_id=e.dispatch_id
      WHERE e.dispatch_proof_verified AND m.environment_id=a.environment_id AND m.email_job_id=a.job_id);
  UPDATE public.novu_rust_sendgrid_recipients r SET delivery_result='unknown'
  WHERE r.delivery_result IS DISTINCT FROM 'unknown'
    AND EXISTS (SELECT 1 FROM inserted_events e
      JOIN public.novu_rust_email_messages m ON m.environment_id=e.environment_id AND m.internal_id=e.message_internal_id
      JOIN public.novu_rust_sendgrid_attempts a ON a.environment_id=m.environment_id AND a.job_id=m.email_job_id AND a.dispatch_id=e.dispatch_id
      WHERE e.dispatch_proof_verified AND r.environment_id=a.environment_id AND r.dispatch_id=a.dispatch_id);
  RETURN NULL;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_invalidate_sendgrid_projection() FROM PUBLIC;
DROP TRIGGER IF EXISTS novu_rust_sendgrid_projection_invalidate ON public.novu_rust_sendgrid_delivery_events;
CREATE TRIGGER novu_rust_sendgrid_projection_invalidate AFTER INSERT ON public.novu_rust_sendgrid_delivery_events
  REFERENCING NEW TABLE AS inserted_events FOR EACH STATEMENT EXECUTE FUNCTION public.novu_rust_invalidate_sendgrid_projection();
