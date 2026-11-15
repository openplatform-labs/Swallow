-- Only bounded numeric 429 Retry-After guidance; no automatic retry.
ALTER TABLE public.novu_rust_twilio_attempts
 ADD COLUMN retry_after_seconds integer CHECK (retry_after_seconds BETWEEN 0 AND 86400),
 ADD COLUMN retry_not_before timestamptz;
