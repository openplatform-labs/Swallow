-- A channel's new projection workers agree on heartbeat semantics.
CREATE TABLE public.novu_rust_projection_worker_policy (
    environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),
    provider text NOT NULL CHECK (provider IN ('sendgrid','resend','twilio')),
    require_full_scan boolean NOT NULL,
    PRIMARY KEY (environment_id, provider)
);
REVOKE ALL ON public.novu_rust_projection_worker_policy FROM PUBLIC;
