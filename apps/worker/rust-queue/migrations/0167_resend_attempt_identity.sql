-- The UUID identifies the current persisted attempt, not a provider message.
ALTER TABLE public.novu_rust_resend_attempts
  ADD COLUMN attempt_id uuid NOT NULL DEFAULT gen_random_uuid();
