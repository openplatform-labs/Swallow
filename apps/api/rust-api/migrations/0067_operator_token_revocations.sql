-- DB administrators register revoked issuer/subject/client/JTI tuples as
-- individual SHA-256 hashes; raw tokens and JWTs are never stored here.
CREATE TABLE IF NOT EXISTS public.novu_rust_operator_token_revocations (
 issuer_hash bytea NOT NULL CHECK(octet_length(issuer_hash)=32),
 subject_hash bytea NOT NULL CHECK(octet_length(subject_hash)=32),
 client_hash bytea NOT NULL CHECK(octet_length(client_hash)=32),
 token_id_hash bytea NOT NULL CHECK(octet_length(token_id_hash)=32),
 reason text NOT NULL CHECK(octet_length(btrim(reason)) BETWEEN 1 AND 1000),
 revoked_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(issuer_hash,subject_hash,client_hash,token_id_hash)
);
REVOKE ALL ON public.novu_rust_operator_token_revocations FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_operator_token_revoked(
 p_issuer bytea,p_subject bytea,p_client bytea,p_token_id bytea
) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF octet_length(p_issuer) IS DISTINCT FROM 32 OR octet_length(p_subject) IS DISTINCT FROM 32
 OR octet_length(p_client) IS DISTINCT FROM 32 OR octet_length(p_token_id) IS DISTINCT FROM 32 THEN
  RAISE EXCEPTION 'invalid token revocation request' USING ERRCODE='22023';
 END IF;
 PERFORM 1 FROM public.novu_rust_operator_jwt_api_logins a
  WHERE a.database_principal=session_user AND a.principal_oid=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin)
  FOR SHARE OF a;
 IF NOT FOUND THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN EXISTS(SELECT 1 FROM public.novu_rust_operator_token_revocations r
  WHERE r.issuer_hash=p_issuer AND r.subject_hash=p_subject AND r.client_hash=p_client AND r.token_id_hash=p_token_id);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_token_revoked(bytea,bytea,bytea,bytea) FROM PUBLIC;
