-- Dispatch-specific, exact-byte SHA-256 hashes. Old sends are not backfilled.
CREATE TABLE IF NOT EXISTS public.novu_rust_sendgrid_recipients (
    dispatch_id uuid NOT NULL REFERENCES public.novu_rust_sendgrid_dispatch_ids(id) ON DELETE CASCADE,
    environment_id text NOT NULL,
    recipient_kind text NOT NULL CHECK (recipient_kind IN ('to','cc','bcc')),
    recipient_index integer NOT NULL CHECK (recipient_index >= 0),
    recipient_hash text NOT NULL CHECK (recipient_hash ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (dispatch_id,recipient_kind,recipient_index)
);
CREATE INDEX IF NOT EXISTS novu_rust_sendgrid_recipient_lookup_idx
    ON public.novu_rust_sendgrid_recipients(environment_id,dispatch_id,recipient_hash);
