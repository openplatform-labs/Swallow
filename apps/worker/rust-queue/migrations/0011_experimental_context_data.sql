-- Context data from a session is insert-only; trigger data may replace it.
ALTER TABLE novu_rust_contexts ADD COLUMN IF NOT EXISTS data jsonb NOT NULL DEFAULT '{}'::jsonb;
