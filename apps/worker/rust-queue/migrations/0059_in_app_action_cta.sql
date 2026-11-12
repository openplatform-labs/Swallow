-- Partial in-app action state; existing messages have no buttons.
ALTER TABLE novu_rust_in_app_messages ADD COLUMN IF NOT EXISTS cta jsonb NOT NULL DEFAULT '{}'::jsonb;
