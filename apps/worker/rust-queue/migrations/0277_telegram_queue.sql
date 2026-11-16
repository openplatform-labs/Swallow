-- Standalone Telegram destinations are permanently bound to one bot-token fingerprint.
CREATE TABLE public.novu_rust_telegram_destinations (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id), destination_id text NOT NULL,
 token_hash text NOT NULL, PRIMARY KEY(environment_id,destination_id)
);
CREATE TABLE public.novu_rust_telegram_attempts (
 job_id uuid PRIMARY KEY REFERENCES public.novu_queue_jobs(id), environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),
 state text NOT NULL CHECK(state IN ('dispatching','accepted','rejected','unknown')),
 provider_message_id text, provider_timestamp text, http_status integer, updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX novu_queue_telegram_pending_idx ON public.novu_queue_jobs(environment_id,(payload->>'destination'),available_at,created_at,id) WHERE kind='experimental_telegram' AND status='pending';
CREATE INDEX novu_queue_telegram_running_idx ON public.novu_queue_jobs(environment_id,(payload->>'destination'),lease_until,id) WHERE kind='experimental_telegram' AND status='running';
REVOKE ALL ON public.novu_rust_telegram_destinations,public.novu_rust_telegram_attempts FROM PUBLIC;
