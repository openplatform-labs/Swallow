ALTER TABLE public.novu_queue_jobs ADD COLUMN IF NOT EXISTS sendgrid_delivery_result text NOT NULL DEFAULT 'unknown';
ALTER TABLE public.novu_rust_email_messages ADD COLUMN IF NOT EXISTS sendgrid_delivery_result text NOT NULL DEFAULT 'unknown';
ALTER TABLE public.novu_rust_sendgrid_recipients ADD COLUMN IF NOT EXISTS delivery_result text NOT NULL DEFAULT 'unknown';
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint WHERE conname='novu_queue_sendgrid_result_check' AND conrelid='public.novu_queue_jobs'::regclass) THEN
    ALTER TABLE public.novu_queue_jobs ADD CONSTRAINT novu_queue_sendgrid_result_check CHECK (sendgrid_delivery_result IN ('unknown','pending','delivered','failed','partial','conflicting'));
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint WHERE conname='novu_email_sendgrid_result_check' AND conrelid='public.novu_rust_email_messages'::regclass) THEN
    ALTER TABLE public.novu_rust_email_messages ADD CONSTRAINT novu_email_sendgrid_result_check CHECK (sendgrid_delivery_result IN ('unknown','pending','delivered','failed','partial','conflicting'));
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint WHERE conname='novu_sendgrid_recipient_result_check' AND conrelid='public.novu_rust_sendgrid_recipients'::regclass) THEN
    ALTER TABLE public.novu_rust_sendgrid_recipients ADD CONSTRAINT novu_sendgrid_recipient_result_check CHECK (delivery_result IN ('unknown','pending','delivered','failed','conflicting'));
  END IF;
END $$;
CREATE TABLE IF NOT EXISTS public.novu_rust_sendgrid_projection_heartbeat (
  environment_id text PRIMARY KEY REFERENCES public.novu_rust_environments(id),
  last_completed_at timestamptz NOT NULL
);
