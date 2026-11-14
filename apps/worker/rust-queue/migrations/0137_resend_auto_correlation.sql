-- Retain the evidence link independently of later deletion of the attempt.
CREATE TABLE IF NOT EXISTS public.novu_rust_resend_auto_correlations (
  job_id uuid PRIMARY KEY,
  environment_id text NOT NULL,
  correlation_id uuid NOT NULL,
  provider_message_id text NOT NULL,
  evidence_count bigint NOT NULL CHECK (evidence_count > 0),
  recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
REVOKE ALL ON public.novu_rust_resend_auto_correlations FROM PUBLIC;
