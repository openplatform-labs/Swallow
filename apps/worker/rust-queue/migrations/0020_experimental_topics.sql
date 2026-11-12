CREATE TABLE IF NOT EXISTS novu_rust_topics (
  environment_id text NOT NULL,
  topic_key text NOT NULL,
  internal_id text NOT NULL,
  name text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (environment_id, topic_key),
  UNIQUE (environment_id, internal_id)
);

CREATE TABLE IF NOT EXISTS novu_rust_topic_subscribers (
  environment_id text NOT NULL,
  topic_key text NOT NULL,
  subscriber_id text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (environment_id, topic_key, subscriber_id),
  FOREIGN KEY (environment_id, topic_key) REFERENCES novu_rust_topics(environment_id, topic_key) ON DELETE CASCADE,
  FOREIGN KEY (environment_id, subscriber_id) REFERENCES novu_rust_subscribers(environment_id, subscriber_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS novu_rust_topic_subscriber_lookup_idx ON novu_rust_topic_subscribers(environment_id, subscriber_id, topic_key);
