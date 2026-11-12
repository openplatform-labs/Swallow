ALTER TABLE novu_rust_subscribers
  ADD COLUMN IF NOT EXISTS is_online boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS last_online_at timestamptz;
