-- Resolve a locally retained token hash only for an authorized environment administrator.
CREATE OR REPLACE FUNCTION public.novu_rust_inspect_console_credential(p_env text,p_hash text)
RETURNS TABLE(credential_id uuid,enabled boolean,expires_at text,created_at text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_env IS NULL OR length(btrim(p_env))=0 OR p_hash IS NULL OR p_hash !~ '^[0-9a-f]{64}$' THEN RAISE EXCEPTION 'invalid console inspect request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_lock_console_admin(p_env);
 RETURN QUERY SELECT c.credential_id,c.enabled,c.expires_at::text,c.created_at::text FROM public.novu_rust_console_credentials c WHERE c.environment_id=p_env AND c.token_hash=p_hash;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_inspect_console_credential(text,text) FROM PUBLIC;
