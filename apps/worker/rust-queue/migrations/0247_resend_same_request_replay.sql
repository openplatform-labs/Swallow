-- Records an optional immediate same-request replay within one dispatch attempt.
ALTER TABLE public.novu_rust_resend_attempts ADD COLUMN same_request_replays integer NOT NULL DEFAULT 0 CHECK (same_request_replays BETWEEN 0 AND 1);
