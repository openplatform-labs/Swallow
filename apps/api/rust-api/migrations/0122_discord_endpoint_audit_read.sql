-- Audit visibility is opt-in and independent of endpoint read/write permissions.
ALTER TABLE public.novu_rust_discord_endpoint_permissions
 DROP CONSTRAINT novu_rust_discord_endpoint_permissions_permission_check;
ALTER TABLE public.novu_rust_discord_endpoint_permissions
 ADD CONSTRAINT novu_rust_discord_endpoint_permissions_permission_check CHECK (permission IN ('read','write','audit'));
CREATE OR REPLACE FUNCTION public.novu_rust_lock_discord_endpoint_grant(p_hash text,p_environment text,p_permission text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
    IF p_permission IS NULL OR p_permission NOT IN ('read','write','audit') THEN RETURN false; END IF;
    PERFORM 1 FROM public.novu_rust_api_keys k JOIN public.novu_rust_users u ON u.id=k.user_id
      JOIN public.novu_rust_discord_endpoint_permissions p ON p.key_hash=k.key_hash
      WHERE k.key_hash=p_hash AND k.environment_id=p_environment AND k.enabled AND p.permission=p_permission
      FOR SHARE OF k,u,p;
    RETURN FOUND;
END;
$$;
REVOKE ALL ON FUNCTION public.novu_rust_lock_discord_endpoint_grant(text,text,text) FROM PUBLIC;

CREATE INDEX novu_rust_discord_endpoint_audit_environment_id_idx ON public.novu_rust_discord_endpoint_audit(environment_id,id);
CREATE FUNCTION public.novu_rust_discord_endpoint_audit_list(p_hash text,p_environment text,p_after uuid)
RETURNS TABLE(id uuid,identifier text,action text,user_id text,recorded_at text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
    IF NOT public.novu_rust_lock_discord_endpoint_grant(p_hash,p_environment,'audit') THEN
        RAISE EXCEPTION 'audit access denied' USING ERRCODE='42501';
    END IF;
    RETURN QUERY SELECT a.id,a.identifier,a.action,a.user_id,a.created_at::text
      FROM public.novu_rust_discord_endpoint_audit a
      WHERE a.environment_id=p_environment AND (p_after IS NULL OR a.id>p_after)
      ORDER BY a.id LIMIT 101;
END;
$$;
REVOKE ALL ON FUNCTION public.novu_rust_discord_endpoint_audit_list(text,text,uuid) FROM PUBLIC;
