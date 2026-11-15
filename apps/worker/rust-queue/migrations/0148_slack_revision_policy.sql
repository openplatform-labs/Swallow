-- Workers agree on the optional fail-closed endpoint revision policy.
ALTER TABLE public.novu_rust_slack_workflow_worker_config DROP CONSTRAINT IF EXISTS novu_rust_slack_workflow_worker_config_endpoint_policy_check;
ALTER TABLE public.novu_rust_slack_workflow_worker_config ADD CONSTRAINT novu_rust_slack_workflow_worker_config_endpoint_policy_check CHECK (endpoint_policy IS NULL OR endpoint_policy IN ('current','snapshot','snapshot-revision'));
