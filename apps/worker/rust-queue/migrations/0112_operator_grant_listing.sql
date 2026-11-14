-- Administrative read paths; ownership and public schema must be trusted.
CREATE INDEX IF NOT EXISTS novu_rust_operator_grants_listing_idx
  ON public.novu_rust_delivery_operator_grants(environment_id, database_principal COLLATE "C", decision COLLATE "C");
CREATE INDEX IF NOT EXISTS novu_rust_operator_grant_audit_listing_idx
  ON public.novu_rust_operator_grant_audit(environment_id, id);

CREATE OR REPLACE FUNCTION public.novu_rust_list_operator_grants(p_environment text, p_after_principal text, p_after_permission text)
RETURNS TABLE(principal text, permission text, identity_current boolean)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  IF p_environment IS NULL OR length(btrim(p_environment))=0
    OR (p_after_principal IS NULL) <> (p_after_permission IS NULL)
    OR (p_after_principal IS NOT NULL AND (octet_length(p_after_principal) NOT BETWEEN 1 AND 63 OR octet_length(p_after_permission) NOT BETWEEN 1 AND 64)) THEN
    RAISE EXCEPTION 'invalid grant listing scope or cursor' USING ERRCODE='22023';
  END IF;
  PERFORM 1 FROM public.novu_rust_operator_grant_admins a
    WHERE a.environment_id=p_environment AND a.database_principal=session_user
      AND a.principal_oid=(SELECT r.oid FROM pg_catalog.pg_roles r WHERE r.rolname=session_user AND r.rolcanlogin)
    FOR SHARE OF a;
  IF NOT FOUND THEN RAISE EXCEPTION 'operator grant administration denied' USING ERRCODE='42501'; END IF;
  RETURN QUERY SELECT g.database_principal, g.decision,
    coalesce(g.principal_oid=r.oid AND r.rolcanlogin,false)
    FROM public.novu_rust_delivery_operator_grants g
    LEFT JOIN pg_catalog.pg_roles r ON r.rolname=g.database_principal
    WHERE g.environment_id=p_environment AND (p_after_principal IS NULL OR
      (g.database_principal COLLATE "C",g.decision COLLATE "C") > (p_after_principal COLLATE "C",p_after_permission COLLATE "C"))
    ORDER BY g.database_principal COLLATE "C",g.decision COLLATE "C" LIMIT 100;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_list_operator_grants(text,text,text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_list_operator_grant_audit(p_environment text,p_after bigint)
RETURNS TABLE(audit_id bigint,actor text,target text,permission text,action text,reason text,created_at text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  IF p_environment IS NULL OR length(btrim(p_environment))=0 OR (p_after IS NOT NULL AND p_after<0) THEN
    RAISE EXCEPTION 'invalid grant audit scope or cursor' USING ERRCODE='22023';
  END IF;
  PERFORM 1 FROM public.novu_rust_operator_grant_admins a
    WHERE a.environment_id=p_environment AND a.database_principal=session_user
      AND a.principal_oid=(SELECT r.oid FROM pg_catalog.pg_roles r WHERE r.rolname=session_user AND r.rolcanlogin)
    FOR SHARE OF a;
  IF NOT FOUND THEN RAISE EXCEPTION 'operator grant administration denied' USING ERRCODE='42501'; END IF;
  RETURN QUERY SELECT a.id,a.actor_session_user,a.target_session_user,a.permission,a.action,a.reason,a.created_at::text
    FROM public.novu_rust_operator_grant_audit a
    WHERE a.environment_id=p_environment AND (p_after IS NULL OR a.id>p_after)
    ORDER BY a.id LIMIT 100;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_list_operator_grant_audit(text,bigint) FROM PUBLIC;
