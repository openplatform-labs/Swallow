-- Integration and connection metadata only: no credentials are stored here.
CREATE TABLE IF NOT EXISTS novu_rust_slack_endpoint_integrations (
    environment_id text NOT NULL REFERENCES novu_rust_environments(id),
    identifier text NOT NULL CHECK (length(identifier) BETWEEN 1 AND 255),
    PRIMARY KEY(environment_id,identifier)
);
CREATE TABLE IF NOT EXISTS novu_rust_slack_endpoint_connections (
    environment_id text NOT NULL,
    integration_identifier text NOT NULL,
    identifier text NOT NULL CHECK (length(identifier) BETWEEN 1 AND 255),
    PRIMARY KEY(environment_id,integration_identifier,identifier),
    FOREIGN KEY(environment_id,integration_identifier)
        REFERENCES novu_rust_slack_endpoint_integrations(environment_id,identifier) ON DELETE RESTRICT
);
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='novu_rust_slack_channel_endpoints'::regclass AND conname='novu_slack_endpoint_integration_fk') THEN
        ALTER TABLE novu_rust_slack_channel_endpoints ADD CONSTRAINT novu_slack_endpoint_integration_fk
          FOREIGN KEY(environment_id,integration_identifier) REFERENCES novu_rust_slack_endpoint_integrations(environment_id,identifier) ON DELETE RESTRICT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='novu_rust_slack_channel_endpoints'::regclass AND conname='novu_slack_endpoint_connection_fk') THEN
        ALTER TABLE novu_rust_slack_channel_endpoints ADD CONSTRAINT novu_slack_endpoint_connection_fk
          FOREIGN KEY(environment_id,integration_identifier,connection_identifier) REFERENCES novu_rust_slack_endpoint_connections(environment_id,integration_identifier,identifier) ON DELETE RESTRICT;
    END IF;
END $$;
