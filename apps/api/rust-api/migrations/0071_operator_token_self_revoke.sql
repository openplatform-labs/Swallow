-- Allow an authenticated, provisioned operator to revoke only its own JTI.
CREATE OR REPLACE FUNCTION public.novu_rust_operator_jwt_self_revoke(
 p_issuer text,p_subject text,p_client text,p_environment text,p_exp bigint,p_token_id_hash bytea
) RETURNS boolean LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_issuer bytea; v_subject bytea; v_client bytea; v_oid oid; v_inserted bigint;
BEGIN
 IF pg_catalog.current_setting('transaction_isolation') <> 'read committed' OR
    p_issuer IS NULL OR p_subject IS NULL OR p_client IS NULL OR p_environment IS NULL OR
    octet_length(p_token_id_hash) IS DISTINCT FROM 32 THEN
  RAISE EXCEPTION 'invalid operator request' USING ERRCODE='42501';
 END IF;
 v_issuer:=sha256(convert_to(p_issuer,'UTF8'));
 v_subject:=sha256(convert_to(p_subject,'UTF8'));
 v_client:=sha256(convert_to(p_client,'UTF8'));
 PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(encode(v_issuer||v_subject||v_client||p_token_id_hash,'hex'),7194057));
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_environment,p_exp);
 INSERT INTO public.novu_rust_operator_token_revocations(issuer_hash,subject_hash,client_hash,token_id_hash,reason)
  VALUES(v_issuer,v_subject,v_client,p_token_id_hash,'self-revocation') ON CONFLICT DO NOTHING;
 GET DIAGNOSTICS v_inserted = ROW_COUNT;
 IF v_inserted=1 THEN
  SELECT oid INTO v_oid FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin;
  INSERT INTO public.novu_rust_operator_token_revocation_audit(issuer_hash,subject_hash,client_hash,token_id_hash,actor,actor_oid,reason)
   VALUES(v_issuer,v_subject,v_client,p_token_id_hash,session_user,v_oid,'self-revocation');
 END IF;
 RETURN v_inserted=1;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_self_revoke(text,text,text,text,bigint,bytea) FROM PUBLIC;
