-- Experimental Inbox metadata; not an import from an existing Novu database.
ALTER TABLE novu_rust_workflows ADD COLUMN IF NOT EXISTS inbox_metadata jsonb;
ALTER TABLE novu_rust_in_app_messages ADD COLUMN IF NOT EXISTS delivered_at timestamptz;
ALTER TABLE novu_rust_in_app_messages ADD COLUMN IF NOT EXISTS tags jsonb NOT NULL DEFAULT '[]'::jsonb;
