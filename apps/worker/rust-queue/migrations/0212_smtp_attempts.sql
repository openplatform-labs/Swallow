-- SMTP has no provider idempotency key. Persist a single attempt before I/O.
CREATE TABLE public.novu_rust_smtp_attempts (
 job_id uuid PRIMARY KEY REFERENCES public.novu_queue_jobs(id) ON DELETE CASCADE,
 environment_id text NOT NULL,
 state text NOT NULL CHECK(state IN ('dispatching','accepted','unknown')),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX novu_rust_smtp_attempts_environment_idx ON public.novu_rust_smtp_attempts(environment_id,state,job_id);
REVOKE ALL ON public.novu_rust_smtp_attempts FROM PUBLIC;
