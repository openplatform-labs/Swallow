-- A delayed retry remains identifiable to new workers even without callback configuration.
ALTER TABLE public.novu_queue_jobs ADD COLUMN twilio_delayed_retry boolean NOT NULL DEFAULT false;
