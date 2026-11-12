ALTER TABLE novu_rust_topics ALTER COLUMN name DROP NOT NULL;
ALTER TABLE novu_rust_topics ADD COLUMN IF NOT EXISTS name_present boolean NOT NULL DEFAULT true;
ALTER TABLE novu_rust_topics ADD COLUMN IF NOT EXISTS data jsonb;
