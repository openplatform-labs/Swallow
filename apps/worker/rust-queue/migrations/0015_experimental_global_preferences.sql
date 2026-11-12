-- Experimental global channel preferences; removed atomically with subscriber.
CREATE TABLE IF NOT EXISTS novu_rust_subscriber_preferences (
    environment_id text NOT NULL,
    subscriber_id text NOT NULL,
    channels jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(channels)='object'),
    PRIMARY KEY (environment_id, subscriber_id),
    FOREIGN KEY (environment_id, subscriber_id)
      REFERENCES novu_rust_subscribers(environment_id, subscriber_id) ON DELETE CASCADE
);
