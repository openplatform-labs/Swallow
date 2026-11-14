-- Track profile edits across every cooperating update path and identify recreations.
ALTER TABLE public.novu_rust_subscribers ADD COLUMN management_generation uuid NOT NULL DEFAULT gen_random_uuid(), ADD COLUMN profile_revision bigint NOT NULL DEFAULT 0 CHECK (profile_revision >= 0);
CREATE FUNCTION public.novu_rust_subscriber_profile_revision() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 NEW.management_generation := OLD.management_generation;
 IF NEW.profile IS DISTINCT FROM OLD.profile THEN NEW.profile_revision := OLD.profile_revision + 1;
 ELSE NEW.profile_revision := OLD.profile_revision; END IF;
 RETURN NEW;
END; $$;
CREATE TRIGGER novu_rust_subscriber_profile_revision BEFORE UPDATE ON public.novu_rust_subscribers FOR EACH ROW EXECUTE FUNCTION public.novu_rust_subscriber_profile_revision();
REVOKE ALL ON FUNCTION public.novu_rust_subscriber_profile_revision() FROM PUBLIC;
