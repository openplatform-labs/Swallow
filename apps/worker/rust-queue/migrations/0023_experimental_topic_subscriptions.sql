-- Preserve subscription identity independently of the subscriber/topic pair.
-- An identifier collision during backfill aborts migration rather than discarding data.
BEGIN;
ALTER TABLE novu_rust_topic_subscribers
  ADD COLUMN IF NOT EXISTS internal_id text,
  ADD COLUMN IF NOT EXISTS identifier text,
  ADD COLUMN IF NOT EXISTS name text,
  ADD COLUMN IF NOT EXISTS context_keys text[],
  ADD COLUMN IF NOT EXISTS updated_at timestamptz;

UPDATE novu_rust_topic_subscribers SET
  internal_id = COALESCE(internal_id, novu_rust_new_objectid()),
  identifier = COALESCE(identifier, 'tk_' || topic_key || ':si_' || subscriber_id),
  updated_at = COALESCE(updated_at, created_at)
WHERE internal_id IS NULL OR identifier IS NULL OR updated_at IS NULL;

ALTER TABLE novu_rust_topic_subscribers
  ALTER COLUMN internal_id SET DEFAULT novu_rust_new_objectid(),
  ALTER COLUMN internal_id SET NOT NULL,
  ALTER COLUMN identifier SET NOT NULL,
  ALTER COLUMN updated_at SET DEFAULT now(),
  ALTER COLUMN updated_at SET NOT NULL;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'novu_rust_topic_subscribers'::regclass
             AND conname = 'novu_rust_topic_subscribers_pkey'
             AND pg_get_constraintdef(oid) <> 'PRIMARY KEY (environment_id, internal_id)') THEN
    ALTER TABLE novu_rust_topic_subscribers DROP CONSTRAINT novu_rust_topic_subscribers_pkey;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'novu_rust_topic_subscribers'::regclass AND contype = 'p') THEN
    ALTER TABLE novu_rust_topic_subscribers ADD PRIMARY KEY (environment_id, internal_id);
  END IF;
END $$;
CREATE UNIQUE INDEX IF NOT EXISTS novu_rust_topic_subscription_identifier_idx
  ON novu_rust_topic_subscribers(environment_id, identifier);
COMMIT;
