-- Context API timestamps; existing prototype rows have no recoverable history.
ALTER TABLE novu_rust_contexts ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now();
ALTER TABLE novu_rust_contexts ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();
