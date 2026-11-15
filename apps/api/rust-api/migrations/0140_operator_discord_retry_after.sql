-- The same retry grant permits the explicit 429-guided retry choice.
CREATE OR REPLACE FUNCTION public.novu_rust_operator_jwt_discord_resolution_access(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_decision text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE permitted boolean;
BEGIN
 IF p_decision IS NULL OR p_decision NOT IN ('accepted','retry','retry_after') THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 SELECT CASE WHEN p_decision='accepted' THEN g.can_accept ELSE g.can_retry END INTO permitted FROM public.novu_rust_operator_jwt_discord_resolution_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT coalesce(permitted,false) OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 PERFORM 1 FROM public.novu_rust_discord_resolvers g JOIN pg_catalog.pg_roles r ON r.oid=g.principal_oid AND r.rolname=g.database_principal AND r.rolcanlogin WHERE g.environment_id=p_env AND g.database_principal=session_user AND CASE WHEN p_decision='accepted' THEN g.can_accept ELSE g.can_retry END FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_discord_resolution_access(text,text,text,text,bigint,text) FROM PUBLIC;
