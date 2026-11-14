-- Acknowledgements are bound to the observed terminal attempt number and state.
CREATE TABLE IF NOT EXISTS public.novu_rust_slack_workflow_acknowledgements (
    id uuid PRIMARY KEY,
    environment_id text NOT NULL,
    job_id uuid NOT NULL REFERENCES public.novu_queue_jobs(id) ON DELETE CASCADE,
    job_attempts integer NOT NULL,
    job_status text NOT NULL,
    job_error text NOT NULL,
    operator_name text NOT NULL,
    reason text NOT NULL,
    database_session_user text NOT NULL DEFAULT session_user,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(job_id,job_attempts,job_status,job_error)
);
REVOKE ALL ON public.novu_rust_slack_workflow_acknowledgements FROM PUBLIC;
