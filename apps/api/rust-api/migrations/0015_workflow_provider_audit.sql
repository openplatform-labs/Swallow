-- Append-only by privilege policy; retain records independently of mutable keys.
CREATE TABLE IF NOT EXISTS public.novu_rust_workflow_provider_audit (
    id uuid PRIMARY KEY,
    environment_id text NOT NULL,
    workflow_identifier text NOT NULL,
    api_key_hash text NOT NULL CHECK (length(api_key_hash)=64),
    user_id text NOT NULL,
    previous_provider text NOT NULL CHECK (previous_provider IN ('sendgrid','resend')),
    new_provider text NOT NULL CHECK (new_provider IN ('sendgrid','resend')),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS novu_rust_workflow_provider_audit_environment_idx
    ON public.novu_rust_workflow_provider_audit(environment_id,recorded_at DESC,id);
