-- Older workers ignore this flag. Quiesce them before relying on shared 503 cooldown.
ALTER TABLE public.novu_rust_push_workflow_worker_config ADD COLUMN shared_unavailable boolean;
