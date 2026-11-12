-- Mongo-compatible opaque 24-hex identifiers for stable context pagination.
CREATE SEQUENCE IF NOT EXISTS novu_rust_context_cursor_seq;
ALTER TABLE novu_rust_contexts ADD COLUMN IF NOT EXISTS internal_id text NOT NULL DEFAULT (
  lpad(to_hex(extract(epoch FROM clock_timestamp())::bigint), 8, '0') ||
  substr(md5(current_database()), 1, 10) ||
  lpad(to_hex(nextval('novu_rust_context_cursor_seq') % 16777216), 6, '0')
);
CREATE UNIQUE INDEX IF NOT EXISTS novu_rust_context_internal_id_idx ON novu_rust_contexts(internal_id);
CREATE INDEX IF NOT EXISTS novu_rust_context_created_cursor_idx ON novu_rust_contexts(environment_id,created_at,internal_id);
CREATE INDEX IF NOT EXISTS novu_rust_context_updated_cursor_idx ON novu_rust_contexts(environment_id,updated_at,internal_id);
