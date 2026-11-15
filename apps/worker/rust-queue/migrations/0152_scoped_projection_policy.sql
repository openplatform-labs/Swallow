-- A separately provisioned login may register only its allowed environment/provider.
CREATE TABLE public.novu_rust_projection_policy_logins (
    environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),
    provider text NOT NULL CHECK (provider IN ('sendgrid','resend','twilio')),
    database_principal text NOT NULL,
    principal_oid oid NOT NULL,
    PRIMARY KEY (environment_id, provider, database_principal)
);
REVOKE ALL ON public.novu_rust_projection_policy_logins FROM PUBLIC;
CREATE FUNCTION public.novu_rust_register_projection_policy(p_environment text, p_provider text, p_full_scan boolean)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_result boolean;
BEGIN
  IF p_environment IS NULL OR p_provider IS NULL OR p_full_scan IS NULL THEN
    RAISE EXCEPTION 'invalid projection policy' USING ERRCODE='22023';
  END IF;
  PERFORM 1 FROM public.novu_rust_projection_policy_logins g
    WHERE g.environment_id=p_environment AND g.provider=p_provider AND g.database_principal=session_user
      AND g.principal_oid=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin)
    FOR SHARE OF g;
  IF NOT FOUND THEN RAISE EXCEPTION 'projection policy access denied' USING ERRCODE='42501'; END IF;
  INSERT INTO public.novu_rust_projection_worker_policy(environment_id,provider,require_full_scan)
    VALUES(p_environment,p_provider,p_full_scan)
    ON CONFLICT(environment_id,provider) DO UPDATE SET require_full_scan=EXCLUDED.require_full_scan
    WHERE public.novu_rust_projection_worker_policy.require_full_scan=EXCLUDED.require_full_scan
    RETURNING true INTO v_result;
  RETURN COALESCE(v_result,false);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_register_projection_policy(text,text,boolean) FROM PUBLIC;
