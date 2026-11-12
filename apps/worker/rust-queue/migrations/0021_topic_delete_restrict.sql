-- A topic with subscribers cannot be deleted. The FK is the final guard when
-- a subscriber is added concurrently with a delete statement's snapshot.
ALTER TABLE novu_rust_topic_subscribers
  DROP CONSTRAINT IF EXISTS novu_rust_topic_subscribers_environment_id_topic_key_fkey;
ALTER TABLE novu_rust_topic_subscribers
  ADD CONSTRAINT novu_rust_topic_subscribers_environment_id_topic_key_fkey
  FOREIGN KEY (environment_id, topic_key)
  REFERENCES novu_rust_topics(environment_id, topic_key) ON DELETE RESTRICT;
