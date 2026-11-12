-- Model MongoDB's dotted-path traversal through arrays for Inbox data filters.
-- SQL NULL is represented as JSON null when an object path is missing.
CREATE OR REPLACE FUNCTION novu_rust_inbox_path_values(p_value jsonb, p_path text[])
RETURNS SETOF jsonb LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
DECLARE
  item jsonb;
  next_path text[];
  index_number integer;
BEGIN
  IF cardinality(p_path) = 0 THEN
    RETURN NEXT COALESCE(p_value, 'null'::jsonb);
    RETURN;
  END IF;
  next_path := p_path[2:cardinality(p_path)];
  IF jsonb_typeof(p_value) = 'object' THEN
    RETURN QUERY SELECT * FROM novu_rust_inbox_path_values(p_value -> p_path[1], next_path);
  ELSIF jsonb_typeof(p_value) = 'array' THEN
    -- A canonical numeric segment can address an element and can also be an
    -- object key in any array element; MongoDB considers both alternatives.
    IF p_path[1] ~ '^(0|[1-9][0-9]{0,8})$' THEN
      index_number := p_path[1]::integer;
      item := p_value -> index_number;
      IF jsonb_typeof(item) IN ('object', 'array') THEN
        RETURN QUERY SELECT * FROM novu_rust_inbox_path_values(item, next_path);
      END IF;
    END IF;
    FOR item IN SELECT value FROM jsonb_array_elements(p_value) LOOP
      IF jsonb_typeof(item) = 'object' THEN
        RETURN QUERY SELECT * FROM novu_rust_inbox_path_values(item, p_path);
      END IF;
    END LOOP;
  ELSE
    RETURN NEXT 'null'::jsonb;
  END IF;
END;
$$;

CREATE OR REPLACE FUNCTION novu_rust_inbox_data_matches(p_data jsonb, p_filter jsonb)
RETURNS boolean LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT NOT EXISTS (
    SELECT 1 FROM jsonb_array_elements(p_filter) AS entry(value)
    CROSS JOIN LATERAL jsonb_array_elements(entry.value->'groups') AS groups(value)
    CROSS JOIN LATERAL (SELECT ARRAY(SELECT jsonb_array_elements_text(entry.value->'path')) AS parts) AS path
    WHERE NOT EXISTS (
      SELECT 1 FROM novu_rust_inbox_path_values(p_data, path.parts) AS actual(value)
      CROSS JOIN jsonb_array_elements(groups.value) AS expected(value)
      WHERE actual.value = expected.value
         OR (jsonb_typeof(actual.value) = 'array' AND actual.value @> jsonb_build_array(expected.value))
    )
  );
$$;
