-- Prototype supports a single in-app step; preserve its active flag separately
-- from the presence of any workflow steps.
ALTER TABLE novu_rust_workflows ADD COLUMN IF NOT EXISTS in_app_step_active boolean NOT NULL DEFAULT true;
