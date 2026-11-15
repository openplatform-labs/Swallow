-- New Resend workers must agree on idempotency scope within an environment.
CREATE TABLE IF NOT EXISTS public.novu_rust_resend_idempotency_policy (
    environment_id text PRIMARY KEY REFERENCES public.novu_rust_environments(id),
    scope text NOT NULL CHECK (scope IN ('job', 'retry'))
);
REVOKE ALL ON public.novu_rust_resend_idempotency_policy FROM PUBLIC;
