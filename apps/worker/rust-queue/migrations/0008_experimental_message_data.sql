-- Inbox data is separate from the trigger payload, including for V0 workflows.
ALTER TABLE novu_rust_in_app_messages ADD COLUMN IF NOT EXISTS data jsonb;

-- The normalized filter contains path arrays and AND-ed groups of OR values.
-- MongoDB equality to null also matches missing fields.
CREATE OR REPLACE FUNCTION novu_rust_inbox_data_matches(p_data jsonb, p_filter jsonb)
RETURNS boolean LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT NOT EXISTS (
    SELECT 1 FROM jsonb_array_elements(p_filter) AS entry(value)
    CROSS JOIN LATERAL jsonb_array_elements(entry.value->'groups') AS groups(value)
    CROSS JOIN LATERAL (SELECT ARRAY(SELECT jsonb_array_elements_text(entry.value->'path')) AS parts) AS path
    CROSS JOIN LATERAL (SELECT p_data #> path.parts AS value) AS actual
    WHERE EXISTS (
      -- Mongo traverses arrays in intermediate dotted paths. Do not treat an
      -- unsupported traversal as a missing value (especially for null filters).
      SELECT 1 FROM generate_series(1, array_length(path.parts, 1) - 1) AS idx
      WHERE jsonb_typeof(p_data #> path.parts[1:idx]) = 'array'
    ) OR NOT EXISTS (
      SELECT 1 FROM jsonb_array_elements(groups.value) AS expected(value)
      WHERE (actual.value IS NULL AND expected.value = 'null'::jsonb)
         OR actual.value = expected.value
         OR (jsonb_typeof(actual.value) = 'array' AND actual.value @> jsonb_build_array(expected.value))
    )
  );
$$;
