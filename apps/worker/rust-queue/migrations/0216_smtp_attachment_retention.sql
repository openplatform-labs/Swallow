-- Include completed workflow-generated SMTP jobs in existing opt-in retention.
CREATE OR REPLACE FUNCTION public.novu_rust_has_workflow_attachment(p_kind text,p_payload jsonb) RETURNS boolean
LANGUAGE sql IMMUTABLE SET search_path=pg_catalog AS $$
 SELECT CASE
  WHEN p_kind IN ('experimental_static_in_app','experimental_static_in_app_attachments_v1') AND jsonb_typeof(p_payload->'emailAttachments')='array' THEN jsonb_array_length(p_payload->'emailAttachments')>0
  WHEN p_kind IN ('experimental_resend_email','experimental_sendgrid_email','experimental_smtp_email') AND p_payload ? 'workflow_context' AND jsonb_typeof(p_payload->'attachments')='array' THEN jsonb_array_length(p_payload->'attachments')>0
  ELSE false END
$$;
REVOKE ALL ON FUNCTION public.novu_rust_has_workflow_attachment(text,jsonb) FROM PUBLIC;
-- Existing completed SMTP jobs receive a fresh retention clock, not their original completion time.
UPDATE public.novu_queue_jobs SET attachment_completed_at=clock_timestamp()
 WHERE kind='experimental_smtp_email' AND status='completed' AND attachment_completed_at IS NULL
 AND attachment_scrubbed_at IS NULL AND public.novu_rust_has_workflow_attachment(kind,payload);
