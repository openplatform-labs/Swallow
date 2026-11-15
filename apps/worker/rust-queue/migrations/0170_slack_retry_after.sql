-- Retain only bounded numeric Retry-After guidance from an HTTP 429 response.
ALTER TABLE public.novu_rust_slack_attempts
 ADD COLUMN IF NOT EXISTS retry_after_seconds integer CHECK (retry_after_seconds BETWEEN 0 AND 86400),
 ADD COLUMN IF NOT EXISTS retry_not_before timestamptz;
