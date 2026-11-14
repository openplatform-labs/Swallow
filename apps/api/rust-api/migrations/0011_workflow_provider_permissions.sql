-- No grants are created automatically. Restrict writes to database administrators.
CREATE TABLE IF NOT EXISTS novu_rust_workflow_provider_permissions (
    key_hash text NOT NULL REFERENCES novu_rust_api_keys(key_hash) ON DELETE CASCADE,
    permission text NOT NULL CHECK (permission IN ('read','write')),
    PRIMARY KEY (key_hash, permission)
);
