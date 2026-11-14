-- New email workers in an environment must select the same step policy.
CREATE TABLE IF NOT EXISTS public.novu_rust_email_workflow_worker_config (
 environment_id text PRIMARY KEY REFERENCES public.novu_rust_environments(id),
 step_policy text CHECK (step_policy IS NULL OR step_policy IN ('snapshot','active','unchanged'))
);
REVOKE ALL ON public.novu_rust_email_workflow_worker_config FROM PUBLIC;
