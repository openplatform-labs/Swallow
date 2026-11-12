-- Cursor traversal for all subscriptions of one environment-scoped subscriber.
CREATE INDEX IF NOT EXISTS novu_rust_subscriber_subscription_cursor_idx
  ON novu_rust_topic_subscribers(environment_id,subscriber_id,internal_id);
