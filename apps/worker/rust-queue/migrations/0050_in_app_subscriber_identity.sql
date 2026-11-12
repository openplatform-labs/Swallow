-- Messages belong to an internal subscriber identity, not a reusable external
-- subscriber ID. Preserve notification identity where available; legacy rows
-- without notification linkage are attributed to their current subscriber.
ALTER TABLE novu_rust_in_app_messages ADD COLUMN IF NOT EXISTS subscriber_internal_id text;
UPDATE novu_rust_in_app_messages m
SET subscriber_internal_id = COALESCE(
    (SELECT n.subscriber_internal_id FROM novu_rust_notifications n
     WHERE n.environment_id=m.environment_id AND n.internal_id=m.notification_internal_id),
    s.internal_id)
FROM novu_rust_subscribers s
WHERE m.subscriber_internal_id IS NULL
  AND s.environment_id=m.environment_id AND s.subscriber_id=m.subscriber_id;
ALTER TABLE novu_rust_in_app_messages ALTER COLUMN subscriber_internal_id SET NOT NULL;

CREATE OR REPLACE FUNCTION novu_rust_in_app_assign_subscriber_identity() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.subscriber_internal_id IS NULL THEN
    SELECT internal_id INTO NEW.subscriber_internal_id FROM novu_rust_subscribers
      WHERE environment_id=NEW.environment_id AND subscriber_id=NEW.subscriber_id;
  END IF;
  RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS novu_rust_in_app_assign_subscriber_identity ON novu_rust_in_app_messages;
CREATE TRIGGER novu_rust_in_app_assign_subscriber_identity
  BEFORE INSERT ON novu_rust_in_app_messages FOR EACH ROW
  EXECUTE FUNCTION novu_rust_in_app_assign_subscriber_identity();

ALTER TABLE novu_rust_in_app_messages
  DROP CONSTRAINT IF EXISTS novu_rust_in_app_messages_environment_id_subscriber_id_fkey;
DO $$
DECLARE old_unique text;
BEGIN
  SELECT conname INTO old_unique FROM pg_constraint
    WHERE conrelid='novu_rust_in_app_messages'::regclass AND contype='u'
      AND pg_get_constraintdef(oid)='UNIQUE (environment_id, subscriber_id, workflow_identifier, transaction_id)';
  IF old_unique IS NOT NULL THEN
    EXECUTE format('ALTER TABLE novu_rust_in_app_messages DROP CONSTRAINT %I',old_unique);
  END IF;
END $$;
CREATE UNIQUE INDEX IF NOT EXISTS novu_rust_in_app_identity_transaction_idx
  ON novu_rust_in_app_messages(environment_id,subscriber_internal_id,workflow_identifier,transaction_id);
CREATE INDEX IF NOT EXISTS novu_rust_in_app_identity_idx
  ON novu_rust_in_app_messages(environment_id,subscriber_internal_id);
