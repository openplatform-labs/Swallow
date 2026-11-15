-- Global revocation authority is separate from API request authorization.
CREATE TABLE IF NOT EXISTS public.novu_rust_operator_token_revocation_admins (
 database_principal name PRIMARY KEY,
 principal_oid oid NOT NULL,
 can_revoke boolean NOT NULL DEFAULT false,
 can_read_audit boolean NOT NULL DEFAULT false
);
CREATE TABLE IF NOT EXISTS public.novu_rust_operator_token_revocation_audit (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 issuer_hash bytea NOT NULL,
 subject_hash bytea NOT NULL,
 client_hash bytea NOT NULL,
 token_id_hash bytea NOT NULL,
 actor name NOT NULL,
 actor_oid oid NOT NULL,
 reason text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
REVOKE ALL ON public.novu_rust_operator_token_revocation_admins,public.novu_rust_operator_token_revocation_audit FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_admin_revoke_operator_token(
 p_issuer bytea,p_subject bytea,p_client bytea,p_token_id bytea,p_reason text
) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_oid oid; v_inserted bigint;
BEGIN
 IF octet_length(p_issuer) IS DISTINCT FROM 32 OR octet_length(p_subject) IS DISTINCT FROM 32
 OR octet_length(p_client) IS DISTINCT FROM 32 OR octet_length(p_token_id) IS DISTINCT FROM 32
 OR p_reason IS NULL OR octet_length(btrim(p_reason)) NOT BETWEEN 1 AND 1000 THEN
  RAISE EXCEPTION 'invalid token revocation request' USING ERRCODE='22023';
 END IF;
 SELECT oid INTO v_oid FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin;
 PERFORM 1 FROM public.novu_rust_operator_token_revocation_admins a
  WHERE a.database_principal=session_user AND a.principal_oid=v_oid AND a.can_revoke FOR SHARE OF a;
 IF NOT FOUND THEN RAISE EXCEPTION 'token revocation denied' USING ERRCODE='42501'; END IF;
 INSERT INTO public.novu_rust_operator_token_revocations(issuer_hash,subject_hash,client_hash,token_id_hash,reason)
  VALUES(p_issuer,p_subject,p_client,p_token_id,btrim(p_reason)) ON CONFLICT DO NOTHING;
 GET DIAGNOSTICS v_inserted = ROW_COUNT;
 IF v_inserted=1 THEN
  INSERT INTO public.novu_rust_operator_token_revocation_audit(issuer_hash,subject_hash,client_hash,token_id_hash,actor,actor_oid,reason)
   VALUES(p_issuer,p_subject,p_client,p_token_id,session_user,v_oid,btrim(p_reason));
 END IF;
 RETURN v_inserted=1;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_admin_revoke_operator_token(bytea,bytea,bytea,bytea,text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_operator_token_revocation_audit_page(p_before bigint,p_limit integer)
RETURNS TABLE(id bigint,issuer_hash bytea,subject_hash bytea,client_hash bytea,token_id_hash bytea,actor name,actor_oid oid,reason text,created_at timestamptz)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_oid oid;
BEGIN
 IF p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 100 OR (p_before IS NOT NULL AND p_before<1) THEN
  RAISE EXCEPTION 'invalid token revocation audit request' USING ERRCODE='22023';
 END IF;
 SELECT oid INTO v_oid FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin;
 PERFORM 1 FROM public.novu_rust_operator_token_revocation_admins a
  WHERE a.database_principal=session_user AND a.principal_oid=v_oid AND a.can_read_audit FOR SHARE OF a;
 IF NOT FOUND THEN RAISE EXCEPTION 'token revocation audit denied' USING ERRCODE='42501'; END IF;
 RETURN QUERY SELECT a.id,a.issuer_hash,a.subject_hash,a.client_hash,a.token_id_hash,a.actor,a.actor_oid,a.reason,a.created_at
  FROM public.novu_rust_operator_token_revocation_audit a WHERE p_before IS NULL OR a.id<p_before ORDER BY a.id DESC LIMIT p_limit;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_token_revocation_audit_page(bigint,integer) FROM PUBLIC;
