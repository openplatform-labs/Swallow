-- Invoke inside the credential write transaction, retaining this lock until commit.
CREATE OR REPLACE FUNCTION public.novu_rust_assert_fcm_registration_guard()
RETURNS boolean LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 LOCK TABLE public.novu_rust_fcm_devices IN ROW EXCLUSIVE MODE;
 IF NOT EXISTS (
   SELECT 1 FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_proc p ON p.oid=t.tgfoid
   WHERE t.tgrelid=to_regclass('public.novu_rust_fcm_devices')
     AND t.tgname='novu_rust_fcm_device_cleanup_generation' AND NOT t.tgisinternal
     AND (t.tgenabled='A' OR (t.tgenabled='O' AND current_setting('session_replication_role')<>'replica'))
     AND p.oid=to_regprocedure('public.novu_rust_bump_fcm_device_cleanup_generation()')
     AND EXISTS (SELECT 1 FROM pg_catalog.pg_attribute a WHERE a.attrelid=t.tgrelid
       AND a.attname='cleanup_generation' AND NOT a.attisdropped AND a.atttypid='uuid'::regtype)
 ) THEN
   RAISE EXCEPTION 'FCM registration protection unavailable' USING ERRCODE='55000';
 END IF;
 RETURN true;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_assert_fcm_registration_guard() FROM PUBLIC;
