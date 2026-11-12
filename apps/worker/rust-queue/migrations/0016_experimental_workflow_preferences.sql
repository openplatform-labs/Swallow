-- Explicit workflow resource preference subset for single in-app steps.
-- NULL channel preference falls back to the workflow-wide value.
CREATE TABLE IF NOT EXISTS novu_rust_workflow_preferences (
    environment_id text NOT NULL,
    workflow_identifier text NOT NULL,
    all_enabled boolean NOT NULL DEFAULT true,
    read_only boolean NOT NULL DEFAULT false,
    in_app_enabled boolean,
    PRIMARY KEY (environment_id, workflow_identifier),
    FOREIGN KEY (environment_id, workflow_identifier)
      REFERENCES novu_rust_workflows(environment_id, identifier) ON DELETE CASCADE
);
