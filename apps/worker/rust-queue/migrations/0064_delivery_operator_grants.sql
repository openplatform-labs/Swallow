-- No default grants: provision these rows using a separately restricted DB role.
CREATE TABLE IF NOT EXISTS novu_rust_delivery_operator_grants (
    environment_id text NOT NULL,
    database_principal text NOT NULL,
    decision text NOT NULL CHECK (decision IN ('retry', 'accepted')),
    PRIMARY KEY (environment_id, database_principal, decision)
);
-- Historical entries cannot be attributed to an authenticated session user.
ALTER TABLE novu_rust_delivery_reconciliations ADD COLUMN IF NOT EXISTS database_session_user text;
