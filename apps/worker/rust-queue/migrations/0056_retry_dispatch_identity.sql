-- Record the dispatch that an operator decided to retry. Old decisions remain unknown.
ALTER TABLE novu_rust_delivery_reconciliations
    ADD COLUMN IF NOT EXISTS retry_dispatch_id uuid;
ALTER TABLE novu_rust_delivery_reconciliations
    ADD COLUMN IF NOT EXISTS decision_order bigint GENERATED ALWAYS AS IDENTITY;
CREATE INDEX IF NOT EXISTS novu_rust_delivery_retry_order_idx
    ON novu_rust_delivery_reconciliations(environment_id, job_id, decision_order DESC)
    WHERE target_index IS NULL AND decision = 'retry';
