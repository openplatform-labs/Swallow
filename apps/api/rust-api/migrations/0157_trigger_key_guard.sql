-- SECURITY DEFINER confines row-lock privileges to this function's owner.
CREATE FUNCTION public.novu_rust_lock_trigger_api_key(p_hash text,p_env text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_hash IS NULL OR p_hash !~ '^[0-9a-f]{64}$' OR p_env IS NULL OR p_env='' THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM 1 FROM public.novu_rust_api_keys k JOIN public.novu_rust_users u ON u.id=k.user_id WHERE k.key_hash=p_hash AND k.environment_id=p_env AND k.enabled FOR SHARE OF k,u;
 RETURN FOUND;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_lock_trigger_api_key(text,text) FROM PUBLIC;
