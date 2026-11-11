-- Experimental fixture schema. This is not the complete Novu data model.
CREATE TABLE novu_rust_users (
    id text PRIMARY KEY
);
CREATE TABLE novu_rust_environments (
    id text PRIMARY KEY,
    organization_id text NOT NULL
);
CREATE TABLE novu_rust_api_keys (
    key_hash text PRIMARY KEY CHECK (length(key_hash) = 64),
    environment_id text NOT NULL REFERENCES novu_rust_environments(id),
    user_id text NOT NULL,
    enabled boolean NOT NULL DEFAULT true
);
CREATE TABLE novu_rust_workflows (
    environment_id text NOT NULL,
    identifier text NOT NULL,
    active boolean NOT NULL,
    has_steps boolean NOT NULL,
    in_app_content text,
    PRIMARY KEY (environment_id, identifier)
);
CREATE TABLE novu_rust_trigger_transactions (
    environment_id text NOT NULL,
    transaction_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (environment_id, transaction_id)
);
