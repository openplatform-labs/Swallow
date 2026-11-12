-- Legacy and compatibility-mode connections remain unleased (NULL).
-- Drain old WS processes before relying on crash cleanup.
ALTER TABLE novu_rust_ws_connections ADD COLUMN IF NOT EXISTS lease_expires_at timestamptz;
CREATE INDEX IF NOT EXISTS novu_rust_ws_connections_expiry_idx
    ON novu_rust_ws_connections(lease_expires_at)
    WHERE lease_expires_at IS NOT NULL;
