-- NULL is a legacy, unregistered policy. New workers agree on one policy per environment.
ALTER TABLE public.novu_rust_slack_workflow_worker_config ADD COLUMN IF NOT EXISTS step_policy text;
ALTER TABLE public.novu_rust_slack_workflow_worker_config DROP CONSTRAINT IF EXISTS novu_rust_slack_workflow_worker_config_step_policy_check;
ALTER TABLE public.novu_rust_slack_workflow_worker_config ADD CONSTRAINT novu_rust_slack_workflow_worker_config_step_policy_check CHECK (step_policy IS NULL OR step_policy IN ('snapshot','active','unchanged'));
