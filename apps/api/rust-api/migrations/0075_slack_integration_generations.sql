-- A fresh generation is assigned whenever a metadata identity is created.
ALTER TABLE public.novu_rust_slack_endpoint_integrations ADD COLUMN IF NOT EXISTS generation uuid NOT NULL DEFAULT gen_random_uuid();
ALTER TABLE public.novu_rust_slack_endpoint_connections ADD COLUMN IF NOT EXISTS generation uuid NOT NULL DEFAULT gen_random_uuid();
