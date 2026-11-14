-- Experimental provider selection; does not implement Novu integration management.
ALTER TABLE novu_rust_workflows ADD COLUMN IF NOT EXISTS email_provider text NOT NULL DEFAULT 'sendgrid';
ALTER TABLE novu_rust_workflows DROP CONSTRAINT IF EXISTS novu_rust_workflows_email_provider_check;
ALTER TABLE novu_rust_workflows ADD CONSTRAINT novu_rust_workflows_email_provider_check CHECK (email_provider IN ('sendgrid','resend'));
