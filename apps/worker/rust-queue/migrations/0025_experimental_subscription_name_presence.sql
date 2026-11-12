-- Existing non-null names were present. Historical SQL NULL values cannot be
-- distinguished as explicitly null or absent and are conservatively absent.
ALTER TABLE novu_rust_topic_subscribers
  ADD COLUMN IF NOT EXISTS name_present boolean NOT NULL DEFAULT false;
UPDATE novu_rust_topic_subscribers SET name_present=true
  WHERE name IS NOT NULL AND NOT name_present;
