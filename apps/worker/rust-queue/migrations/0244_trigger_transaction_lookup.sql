-- Lookup is restricted by environment and by the two parent workflow job kinds.
CREATE INDEX IF NOT EXISTS novu_queue_trigger_transaction_lookup_idx
  ON public.novu_queue_jobs (environment_id, md5(payload->>'transactionId'))
  WHERE kind IN ('experimental_static_in_app','experimental_static_in_app_attachments_v1');
