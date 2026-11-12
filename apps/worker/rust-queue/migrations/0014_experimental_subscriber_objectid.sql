-- Mongo ObjectIds use 4-byte seconds, a 5-byte process identifier and a
-- 3-byte counter. Share a random installation prefix and counter across API
-- and worker processes; existing IDs are intentionally left unchanged.
CREATE TABLE IF NOT EXISTS novu_rust_objectid_identity (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  prefix text NOT NULL CHECK (prefix ~ '^[0-9a-f]{10}$')
);
INSERT INTO novu_rust_objectid_identity(singleton,prefix)
VALUES (true, substr(replace(gen_random_uuid()::text,'-',''),1,10))
ON CONFLICT (singleton) DO NOTHING;
CREATE SEQUENCE IF NOT EXISTS novu_rust_objectid_counter MINVALUE 0 MAXVALUE 16777215 CYCLE;
CREATE OR REPLACE FUNCTION novu_rust_new_objectid() RETURNS text
LANGUAGE sql VOLATILE AS $$
  SELECT lpad(to_hex(floor(extract(epoch FROM clock_timestamp()))::bigint),8,'0') || prefix ||
         lpad(to_hex(nextval('novu_rust_objectid_counter')),6,'0')
  FROM novu_rust_objectid_identity WHERE singleton
$$;
CREATE UNIQUE INDEX IF NOT EXISTS novu_rust_subscriber_internal_id_idx
  ON novu_rust_subscribers(environment_id,internal_id);
CREATE INDEX IF NOT EXISTS novu_rust_subscriber_updated_cursor_idx
  ON novu_rust_subscribers(environment_id,updated_at,internal_id);
