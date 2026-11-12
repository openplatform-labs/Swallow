-- Supports tenant-scoped topic membership listing and its ObjectId cursor.
CREATE INDEX IF NOT EXISTS novu_rust_topic_subscription_cursor_idx
  ON novu_rust_topic_subscribers(environment_id, topic_key, internal_id);
