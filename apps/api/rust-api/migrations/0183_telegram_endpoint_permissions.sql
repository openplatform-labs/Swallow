-- Explicit API-key permissions; grant EXECUTE only to the API database role.
CREATE TABLE IF NOT EXISTS public.novu_rust_telegram_endpoint_permissions (
    key_hash text NOT NULL REFERENCES public.novu_rust_api_keys(key_hash) ON DELETE CASCADE,
    permission text NOT NULL CHECK (permission IN ('read','write','audit')),
    PRIMARY KEY (key_hash,permission)
);
CREATE TABLE IF NOT EXISTS public.novu_rust_telegram_endpoint_audit (
    id uuid PRIMARY KEY,
    environment_id text NOT NULL,
    identifier text NOT NULL,
    action text NOT NULL CHECK (action IN ('create','update','delete')),
    key_hash text NOT NULL,
    user_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE OR REPLACE FUNCTION public.novu_rust_lock_telegram_endpoint_grant(p_hash text,p_environment text,p_permission text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
    IF p_permission IS NULL OR p_permission NOT IN ('read','write','audit') THEN RETURN false; END IF;
    PERFORM 1 FROM public.novu_rust_api_keys k JOIN public.novu_rust_users u ON u.id=k.user_id
      JOIN public.novu_rust_telegram_endpoint_permissions p ON p.key_hash=k.key_hash
      WHERE k.key_hash=p_hash AND k.environment_id=p_environment AND k.enabled AND p.permission=p_permission
      FOR SHARE OF k,u,p;
    RETURN FOUND;
END;
$$;
REVOKE ALL ON FUNCTION public.novu_rust_lock_telegram_endpoint_grant(text,text,text) FROM PUBLIC;

CREATE INDEX novu_rust_telegram_endpoint_audit_environment_id_idx ON public.novu_rust_telegram_endpoint_audit(environment_id,id);
CREATE FUNCTION public.novu_rust_telegram_endpoint_audit_list(p_hash text,p_environment text,p_after uuid)
RETURNS TABLE(id uuid,identifier text,action text,user_id text,recorded_at text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
    IF NOT public.novu_rust_lock_telegram_endpoint_grant(p_hash,p_environment,'audit') THEN
        RAISE EXCEPTION 'audit access denied' USING ERRCODE='42501';
    END IF;
    RETURN QUERY SELECT a.id,a.identifier,a.action,a.user_id,a.created_at::text
      FROM public.novu_rust_telegram_endpoint_audit a
      WHERE a.environment_id=p_environment AND (p_after IS NULL OR a.id>p_after)
      ORDER BY a.id LIMIT 101;
END;
$$;
REVOKE ALL ON FUNCTION public.novu_rust_telegram_endpoint_audit_list(text,text,uuid) FROM PUBLIC;
