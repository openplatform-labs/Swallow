-- Every update advances an opaque revision, including writes by older clients.
ALTER TABLE public.novu_rust_fcm_integrations ADD COLUMN IF NOT EXISTS revision uuid NOT NULL DEFAULT gen_random_uuid();
CREATE OR REPLACE FUNCTION public.novu_rust_fcm_integration_revision_update()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
  NEW.revision := gen_random_uuid();
  RETURN NEW;
END; $$;
DROP TRIGGER IF EXISTS novu_rust_fcm_integration_revision_update ON public.novu_rust_fcm_integrations;
CREATE TRIGGER novu_rust_fcm_integration_revision_update BEFORE UPDATE ON public.novu_rust_fcm_integrations
FOR EACH ROW EXECUTE FUNCTION public.novu_rust_fcm_integration_revision_update();
