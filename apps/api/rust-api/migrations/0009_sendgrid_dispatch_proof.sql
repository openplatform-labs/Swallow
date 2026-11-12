-- Older events may have a dispatch ID without proof, and stay conservative.
ALTER TABLE novu_rust_sendgrid_delivery_events
    ADD COLUMN IF NOT EXISTS dispatch_proof_verified boolean NOT NULL DEFAULT false;
