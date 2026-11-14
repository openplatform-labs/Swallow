-- Apply as trusted owner; no API-key grants or function execution rights by default.
CREATE TABLE IF NOT EXISTS public.novu_rust_slack_endpoint_permissions (
    key_hash text NOT NULL REFERENCES public.novu_rust_api_keys(key_hash) ON DELETE CASCADE,
    permission text NOT NULL CHECK (permission IN ('read','write')),
    PRIMARY KEY(key_hash,permission)
);
CREATE TABLE IF NOT EXISTS public.novu_rust_slack_endpoint_audit (
    id uuid PRIMARY KEY,
    environment_id text NOT NULL,
    identifier text NOT NULL,
    action text NOT NULL CHECK (action IN ('create','delete')),
    key_hash text NOT NULL,
    user_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE OR REPLACE FUNCTION public.novu_rust_lock_slack_endpoint_grant(p_hash text,p_environment text,p_permission text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
    IF p_permission NOT IN ('read','write') THEN RETURN false; END IF;
    PERFORM 1 FROM public.novu_rust_api_keys k JOIN public.novu_rust_users u ON u.id=k.user_id
      JOIN public.novu_rust_slack_endpoint_permissions p ON p.key_hash=k.key_hash
      WHERE k.key_hash=p_hash AND k.environment_id=p_environment AND k.enabled AND p.permission=p_permission
      FOR SHARE OF k,u,p;
    RETURN FOUND;
END;
$$;
REVOKE ALL ON FUNCTION public.novu_rust_lock_slack_endpoint_grant(text,text,text) FROM PUBLIC;
