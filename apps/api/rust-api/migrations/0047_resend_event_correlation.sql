-- Store only the optional random dispatch tag, not the complete webhook body.
ALTER TABLE public.novu_rust_resend_delivery_events ADD COLUMN IF NOT EXISTS correlation_id uuid;
CREATE INDEX IF NOT EXISTS novu_rust_resend_event_correlation_idx
  ON public.novu_rust_resend_delivery_events(environment_id,correlation_id,email_id)
  WHERE correlation_id IS NOT NULL;
