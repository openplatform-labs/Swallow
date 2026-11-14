-- Each changed device row receives a fresh generation, including delete/reinsert.
ALTER TABLE public.novu_rust_fcm_devices ADD COLUMN IF NOT EXISTS cleanup_generation uuid NOT NULL DEFAULT gen_random_uuid();
CREATE OR REPLACE FUNCTION public.novu_rust_bump_fcm_device_cleanup_generation()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 NEW.cleanup_generation := gen_random_uuid();
 RETURN NEW;
END; $$;
DROP TRIGGER IF EXISTS novu_rust_fcm_device_cleanup_generation ON public.novu_rust_fcm_devices;
CREATE TRIGGER novu_rust_fcm_device_cleanup_generation BEFORE UPDATE ON public.novu_rust_fcm_devices
FOR EACH ROW EXECUTE FUNCTION public.novu_rust_bump_fcm_device_cleanup_generation();
