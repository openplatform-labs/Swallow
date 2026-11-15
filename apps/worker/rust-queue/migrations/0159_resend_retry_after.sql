-- Keep only bounded, validated Retry-After metadata; never store provider headers.
ALTER TABLE public.novu_rust_resend_attempts
 ADD COLUMN IF NOT EXISTS retry_after_seconds integer CHECK (retry_after_seconds BETWEEN 0 AND 86400),
 ADD COLUMN IF NOT EXISTS retry_not_before timestamptz;
