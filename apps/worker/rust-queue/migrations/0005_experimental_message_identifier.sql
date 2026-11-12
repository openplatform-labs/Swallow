-- Internal Mongo-shaped public identifiers for experimental inbox messages.
-- Existing UUIDs are deterministically mapped; this is not a Novu data migration.
ALTER TABLE novu_rust_in_app_messages ADD COLUMN IF NOT EXISTS internal_id text;
UPDATE novu_rust_in_app_messages SET internal_id = substr(md5(id::text), 1, 24) WHERE internal_id IS NULL;
ALTER TABLE novu_rust_in_app_messages ALTER COLUMN internal_id SET NOT NULL;
ALTER TABLE novu_rust_in_app_messages ALTER COLUMN internal_id SET DEFAULT substr(md5(random()::text || clock_timestamp()::text), 1, 24);
CREATE UNIQUE INDEX IF NOT EXISTS novu_rust_in_app_message_internal_id_idx ON novu_rust_in_app_messages (environment_id, internal_id);
