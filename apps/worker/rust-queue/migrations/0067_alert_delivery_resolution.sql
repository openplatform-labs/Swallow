-- Separate alert decisions from notification-job decisions; no default grants.
ALTER TABLE novu_rust_delivery_operator_grants DROP CONSTRAINT IF EXISTS novu_rust_delivery_operator_grants_decision_check;
ALTER TABLE novu_rust_delivery_operator_grants ADD CONSTRAINT novu_rust_delivery_operator_grants_decision_check CHECK (decision IN ('retry','accepted','alert_send','alert_retry','alert_accepted'));
CREATE TABLE IF NOT EXISTS novu_rust_alert_delivery_resolutions (
    id uuid PRIMARY KEY,
    environment_id text NOT NULL,
    channel text NOT NULL,
    target_id text NOT NULL,
    attempt_id uuid NOT NULL REFERENCES novu_rust_alert_delivery_attempts(id),
    previous_state text NOT NULL CHECK (previous_state IN ('unknown','rejected')),
    decision text NOT NULL CHECK (decision IN ('retry','accepted')),
    operator_name text NOT NULL,
    reason text NOT NULL,
    database_user text NOT NULL DEFAULT current_user,
    database_session_user text NOT NULL DEFAULT session_user,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
