-- Bounded provider advice per target. No automatic retry is enabled.
ALTER TABLE public.novu_rust_fcm_attempts
 ADD COLUMN retry_after_seconds integer CHECK (retry_after_seconds BETWEEN 0 AND 86400),
 ADD COLUMN retry_not_before timestamptz;
