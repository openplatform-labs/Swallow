-- Requires worker migrations 0046, 0047 and 0051. Audit and grants belong
-- to a trusted owner; grant only the required rights to the API DB role.
CREATE UNIQUE INDEX IF NOT EXISTS novu_rust_fcm_integration_identifier_idx
  ON public.novu_rust_fcm_integrations(environment_id, identifier)
  WHERE identifier IS NOT NULL;
CREATE TABLE IF NOT EXISTS public.novu_rust_fcm_integration_permissions (
  key_hash text NOT NULL REFERENCES public.novu_rust_api_keys(key_hash) ON DELETE CASCADE,
  permission text NOT NULL CHECK (permission IN ('read','write')),
  PRIMARY KEY(key_hash, permission)
);
CREATE TABLE IF NOT EXISTS public.novu_rust_fcm_integration_audit (
  id uuid PRIMARY KEY,
  environment_id text NOT NULL,
  integration_internal_id text NOT NULL,
  identifier text NOT NULL,
  action text NOT NULL CHECK (action IN ('create','update')),
  old_active boolean,
  new_active boolean NOT NULL,
  key_hash text NOT NULL,
  user_id text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE OR REPLACE FUNCTION public.novu_rust_lock_fcm_integration_grant(p_hash text,p_environment text,p_permission text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  IF p_permission NOT IN ('read','write') THEN RETURN false; END IF;
  PERFORM 1 FROM public.novu_rust_api_keys k JOIN public.novu_rust_users u ON u.id=k.user_id
    JOIN public.novu_rust_fcm_integration_permissions p ON p.key_hash=k.key_hash
    WHERE k.key_hash=p_hash AND k.environment_id=p_environment AND k.enabled AND p.permission=p_permission
    FOR SHARE OF k,u,p;
  RETURN FOUND;
END;
$$;
REVOKE ALL ON FUNCTION public.novu_rust_lock_fcm_integration_grant(text,text,text) FROM PUBLIC;
