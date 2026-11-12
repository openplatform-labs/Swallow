ALTER TABLE novu_rust_email_step_outcomes
  DROP CONSTRAINT IF EXISTS novu_rust_email_step_outcomes_status_check;
ALTER TABLE novu_rust_email_step_outcomes
  ADD CONSTRAINT novu_rust_email_step_outcomes_status_check CHECK (status IN ('queued','canceled','missing_email','failed'));
