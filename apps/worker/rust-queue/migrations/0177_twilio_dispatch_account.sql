-- Existing dispatches have unknown account identity; new workers record it.
ALTER TABLE public.novu_rust_twilio_dispatch_ids ADD COLUMN account_sid text;
