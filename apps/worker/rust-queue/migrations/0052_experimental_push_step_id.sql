-- Step identity used to select trigger provider overrides for a manual push workflow.
ALTER TABLE novu_rust_workflows ADD COLUMN IF NOT EXISTS push_step_id text;
