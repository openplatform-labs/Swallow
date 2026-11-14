-- A new random identifier per dispatch, never reconstructed for old attempts.
ALTER TABLE public.novu_rust_resend_attempts ADD COLUMN IF NOT EXISTS correlation_id uuid;
CREATE UNIQUE INDEX IF NOT EXISTS novu_rust_resend_attempt_correlation_idx
  ON public.novu_rust_resend_attempts(correlation_id) WHERE correlation_id IS NOT NULL;
