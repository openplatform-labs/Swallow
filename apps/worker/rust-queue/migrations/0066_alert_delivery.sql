-- Alert slots coordinate multiple schedulers; credentials are never stored here.
CREATE TABLE IF NOT EXISTS novu_rust_alert_delivery_slots (
    environment_id text NOT NULL,
    channel text NOT NULL CHECK (channel IN ('slack','email')),
    target_id text NOT NULL,
    state text NOT NULL CHECK (state IN ('idle','dispatching','accepted','rejected','unknown')),
    attempt_id uuid,
    last_started_at timestamptz,
    lease_until timestamptz,
    PRIMARY KEY(environment_id,channel,target_id),
    CHECK ((state='dispatching') = (lease_until IS NOT NULL))
);
CREATE TABLE IF NOT EXISTS novu_rust_alert_delivery_attempts (
    id uuid PRIMARY KEY,
    environment_id text NOT NULL,
    channel text NOT NULL,
    target_id text NOT NULL,
    state text NOT NULL CHECK (state IN ('dispatching','accepted','rejected','unknown')),
    http_status integer,
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz,
    FOREIGN KEY(environment_id,channel,target_id) REFERENCES novu_rust_alert_delivery_slots(environment_id,channel,target_id)
);
