ALTER TABLE novu_rust_email_messages
  DROP CONSTRAINT IF EXISTS novu_rust_email_messages_status_check;
ALTER TABLE novu_rust_email_messages
  ADD CONSTRAINT novu_rust_email_messages_status_check CHECK (status IN ('sent','warning','error'));
ALTER TABLE novu_rust_email_messages ADD COLUMN IF NOT EXISTS identifier text;
