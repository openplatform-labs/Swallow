-- Preserve the distinction between a missing MongoDB field and explicit null.
ALTER TABLE novu_rust_in_app_messages
  ADD COLUMN IF NOT EXISTS severity text,
  ADD COLUMN IF NOT EXISTS severity_present boolean NOT NULL DEFAULT false;
ALTER TABLE novu_rust_in_app_messages
  ADD CONSTRAINT novu_rust_message_severity_check
  CHECK ((severity IS NULL OR severity IN ('none', 'low', 'medium', 'high'))
         AND (severity_present OR severity IS NULL));
