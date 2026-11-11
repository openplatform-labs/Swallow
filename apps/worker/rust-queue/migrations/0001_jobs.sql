-- Queue primitives only; not a mapping of Novu's complete job model.
CREATE TABLE IF NOT EXISTS novu_queue_jobs (
    id uuid PRIMARY KEY,
    environment_id text NOT NULL,
    kind text NOT NULL,
    payload jsonb NOT NULL,
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
    available_at timestamptz NOT NULL DEFAULT now(),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    lease_token uuid,
    lease_until timestamptz,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK ((status = 'running') = (lease_token IS NOT NULL AND lease_until IS NOT NULL)),
    CHECK ((lease_token IS NULL) = (lease_until IS NULL))
);

CREATE INDEX IF NOT EXISTS novu_queue_pending_idx
    ON novu_queue_jobs (kind, available_at, created_at, id) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS novu_queue_lease_idx
    ON novu_queue_jobs (lease_until, id) WHERE status = 'running';
