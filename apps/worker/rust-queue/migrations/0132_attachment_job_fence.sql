-- Route pending attachment work away from workers that predate attachment support.
CREATE FUNCTION public.novu_rust_fence_attachment_job() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 NEW.kind:='experimental_static_in_app_attachments_v1';
 RETURN NEW;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_fence_attachment_job() FROM PUBLIC;
CREATE TRIGGER novu_rust_fence_attachment_job BEFORE INSERT OR UPDATE OF kind,status,payload ON public.novu_queue_jobs
FOR EACH ROW WHEN (
 NEW.kind='experimental_static_in_app' AND NEW.status='pending' AND
 CASE WHEN jsonb_typeof(NEW.payload->'emailAttachments')='array'
 THEN jsonb_array_length(NEW.payload->'emailAttachments')>0 ELSE NEW.payload ? 'emailAttachments' END
) EXECUTE FUNCTION public.novu_rust_fence_attachment_job();
-- Existing running work cannot safely be intercepted; see deployment instructions.
UPDATE public.novu_queue_jobs SET kind='experimental_static_in_app_attachments_v1'
WHERE kind='experimental_static_in_app' AND status='pending' AND
 CASE WHEN jsonb_typeof(payload->'emailAttachments')='array'
 THEN jsonb_array_length(payload->'emailAttachments')>0 ELSE payload ? 'emailAttachments' END;
