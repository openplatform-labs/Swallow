-- Only a hash is retained; pre-upgrade dispatches have no verified proof.
ALTER TABLE novu_rust_sendgrid_dispatch_ids ADD COLUMN IF NOT EXISTS proof_hash text;
