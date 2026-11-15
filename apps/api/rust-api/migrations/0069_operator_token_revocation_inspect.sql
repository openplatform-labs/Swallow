-- Read permission is separate from the authority to revoke tokens.
CREATE OR REPLACE FUNCTION public.novu_rust_operator_token_revocation_inspect(
 p_issuer bytea,p_subject bytea,p_client bytea,p_token_id bytea
) RETURNS TABLE(revoked boolean,reason text,revoked_at timestamptz)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_oid oid;
BEGIN
 IF octet_length(p_issuer) IS DISTINCT FROM 32 OR octet_length(p_subject) IS DISTINCT FROM 32
 OR octet_length(p_client) IS DISTINCT FROM 32 OR octet_length(p_token_id) IS DISTINCT FROM 32 THEN
  RAISE EXCEPTION 'invalid token revocation request' USING ERRCODE='22023';
 END IF;
 SELECT oid INTO v_oid FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin;
 PERFORM 1 FROM public.novu_rust_operator_token_revocation_admins a
  WHERE a.database_principal=session_user AND a.principal_oid=v_oid AND a.can_read_audit FOR SHARE OF a;
 IF NOT FOUND THEN RAISE EXCEPTION 'token revocation inspection denied' USING ERRCODE='42501'; END IF;
 RETURN QUERY SELECT true,r.reason,r.revoked_at FROM public.novu_rust_operator_token_revocations r
  WHERE r.issuer_hash=p_issuer AND r.subject_hash=p_subject AND r.client_hash=p_client AND r.token_id_hash=p_token_id;
 IF NOT FOUND THEN RETURN QUERY SELECT false,NULL::text,NULL::timestamptz; END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_token_revocation_inspect(bytea,bytea,bytea,bytea) FROM PUBLIC;
