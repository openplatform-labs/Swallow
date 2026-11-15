-- Read-only recheck of the login grant and registered policy before projection work.
CREATE FUNCTION public.novu_rust_check_projection_policy(p_environment text, p_provider text, p_full_scan boolean)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  IF p_environment IS NULL OR p_provider IS NULL OR p_full_scan IS NULL THEN
    RAISE EXCEPTION 'invalid projection policy' USING ERRCODE='22023';
  END IF;
  PERFORM 1 FROM public.novu_rust_projection_policy_logins g
    WHERE g.environment_id=p_environment AND g.provider=p_provider AND g.database_principal=session_user
      AND g.principal_oid=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin)
    FOR SHARE OF g;
  IF NOT FOUND THEN RAISE EXCEPTION 'projection policy access denied' USING ERRCODE='42501'; END IF;
  PERFORM 1 FROM public.novu_rust_projection_worker_policy p
    WHERE p.environment_id=p_environment AND p.provider=p_provider AND p.require_full_scan=p_full_scan
    FOR SHARE OF p;
  RETURN FOUND;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_check_projection_policy(text,text,boolean) FROM PUBLIC;
