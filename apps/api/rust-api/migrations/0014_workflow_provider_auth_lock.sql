-- Install as a trusted owner of the public key, user and permission tables.
-- This function checks the supplied key hash and takes only row locks.
CREATE OR REPLACE FUNCTION public.novu_rust_lock_workflow_provider_grant(p_hash text, p_environment text, p_permission text)
RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    IF p_permission NOT IN ('read', 'write') THEN RETURN false; END IF;
    PERFORM 1 FROM public.novu_rust_api_keys k
      JOIN public.novu_rust_users u ON u.id=k.user_id
      JOIN public.novu_rust_workflow_provider_permissions p ON p.key_hash=k.key_hash
      WHERE k.key_hash=p_hash AND k.environment_id=p_environment AND k.enabled AND p.permission=p_permission
      FOR SHARE OF k,u,p;
    RETURN FOUND;
END;
$$;
REVOKE ALL ON FUNCTION public.novu_rust_lock_workflow_provider_grant(text,text,text) FROM PUBLIC;
