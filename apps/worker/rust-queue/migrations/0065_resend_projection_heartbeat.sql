-- A completed full scan, not a guarantee that every job was unlocked.
CREATE TABLE IF NOT EXISTS novu_rust_resend_projection_heartbeat (
    environment_id text PRIMARY KEY,
    last_completed_at timestamptz NOT NULL
);
