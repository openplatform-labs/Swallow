-- Subscriber deletion removes each channel's history by environment and the
-- subscriber's original internal identity, rather than by reused external ID.
CREATE INDEX IF NOT EXISTS novu_rust_email_messages_subscriber_idx
    ON novu_rust_email_messages(environment_id, subscriber_internal_id);
CREATE INDEX IF NOT EXISTS novu_rust_sms_messages_subscriber_idx
    ON novu_rust_sms_messages(environment_id, subscriber_internal_id);
CREATE INDEX IF NOT EXISTS novu_rust_push_messages_subscriber_idx
    ON novu_rust_push_messages(environment_id, subscriber_internal_id);
