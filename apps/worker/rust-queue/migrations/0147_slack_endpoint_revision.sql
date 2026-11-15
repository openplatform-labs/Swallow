-- Preserve endpoint identity while tracking intervening changes.
ALTER TABLE public.novu_rust_slack_channel_endpoints ADD COLUMN IF NOT EXISTS revision uuid NOT NULL DEFAULT gen_random_uuid();
CREATE OR REPLACE FUNCTION public.novu_rust_slack_endpoint_revision_update()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
  NEW.revision := gen_random_uuid();
  RETURN NEW;
END; $$;
DROP TRIGGER IF EXISTS novu_rust_slack_endpoint_revision_update ON public.novu_rust_slack_channel_endpoints;
CREATE TRIGGER novu_rust_slack_endpoint_revision_update BEFORE UPDATE ON public.novu_rust_slack_channel_endpoints
FOR EACH ROW EXECUTE FUNCTION public.novu_rust_slack_endpoint_revision_update();
