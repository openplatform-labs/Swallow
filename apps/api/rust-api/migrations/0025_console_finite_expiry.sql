ALTER TABLE public.novu_rust_console_credentials ADD CONSTRAINT novu_rust_console_finite_expiry CHECK (isfinite(expires_at));
