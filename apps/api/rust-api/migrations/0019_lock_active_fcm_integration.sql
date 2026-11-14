-- Trusted owner must have access to the worker-owned FCM integration table.
-- The shared row lock lasts until the caller's transaction ends.
CREATE OR REPLACE FUNCTION public.novu_rust_lock_active_fcm_integration(p_environment text,p_internal_id text,p_identifier text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  PERFORM 1 FROM public.novu_rust_fcm_integrations i
    WHERE i.environment_id=p_environment AND i.internal_id=p_internal_id AND i.active
      AND (p_identifier IS NULL OR p_identifier='' OR i.identifier=p_identifier)
    FOR SHARE;
  RETURN FOUND;
END;
$$;
REVOKE ALL ON FUNCTION public.novu_rust_lock_active_fcm_integration(text,text,text) FROM PUBLIC;
