-- Explicit SMTP selection for workflow-generated email jobs.
ALTER TABLE public.novu_rust_workflows DROP CONSTRAINT IF EXISTS novu_rust_workflows_email_provider_check;
ALTER TABLE public.novu_rust_workflows ADD CONSTRAINT novu_rust_workflows_email_provider_check CHECK (email_provider IN ('sendgrid','resend','smtp'));
-- Old generators treat every non-Resend provider as SendGrid. Reject their
-- new SendGrid jobs for SMTP workflows; this cannot recall already queued work.
CREATE FUNCTION public.novu_rust_fence_smtp_workflow_job() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF NEW.kind='experimental_sendgrid_email' AND NEW.payload->'workflow_context' IS NOT NULL AND NEW.payload->>'workflow_identifier' IS NOT NULL THEN
  PERFORM 1 FROM public.novu_rust_workflows w WHERE w.environment_id=NEW.environment_id AND w.identifier=NEW.payload->>'workflow_identifier' AND w.email_provider='smtp' FOR SHARE OF w;
  IF FOUND THEN RAISE EXCEPTION 'SMTP workflow requires compatible generator' USING ERRCODE='55000'; END IF;
 END IF;
 RETURN NEW;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_fence_smtp_workflow_job() FROM PUBLIC;
CREATE TRIGGER novu_rust_fence_smtp_workflow_job BEFORE INSERT ON public.novu_queue_jobs FOR EACH ROW EXECUTE FUNCTION public.novu_rust_fence_smtp_workflow_job();
