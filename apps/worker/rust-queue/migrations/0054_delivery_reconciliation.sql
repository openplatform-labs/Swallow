-- Decision evidence; restrict update/delete and table access using DB privileges.
CREATE TABLE IF NOT EXISTS novu_rust_delivery_reconciliations (
    id uuid PRIMARY KEY,
    environment_id text NOT NULL,
    job_id uuid NOT NULL REFERENCES novu_queue_jobs(id),
    target_index integer CHECK (target_index IS NULL OR target_index >= 0),
    decision text NOT NULL CHECK (decision IN ('retry','accepted')),
    operator_name text NOT NULL CHECK (length(operator_name) BETWEEN 1 AND 128),
    database_user text NOT NULL DEFAULT current_user,
    reason text NOT NULL CHECK (length(reason) BETWEEN 1 AND 512),
    previous_http_status integer,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS novu_rust_delivery_reconciliations_job_idx ON novu_rust_delivery_reconciliations(environment_id,job_id,created_at);
