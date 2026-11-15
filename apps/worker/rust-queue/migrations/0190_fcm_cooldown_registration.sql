-- New workers register the chosen shared-cooldown mode alongside other push policies.
-- NULL lets the first upgraded worker bind the value; it does not fence legacy binaries.
ALTER TABLE public.novu_rust_push_workflow_worker_config ADD COLUMN shared_cooldown boolean;
