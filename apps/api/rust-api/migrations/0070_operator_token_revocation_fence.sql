-- Share a transaction-scoped lock between guarded operations and denylist
-- writes. The hash is a lock namespace, not the stored token identity.
CREATE OR REPLACE FUNCTION public.novu_rust_operator_token_guard(p_issuer bytea,p_subject bytea,p_client bytea,p_token_id bytea)
RETURNS boolean LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF pg_catalog.current_setting('transaction_isolation') <> 'read committed' THEN
  RAISE EXCEPTION 'token guard requires read committed isolation' USING ERRCODE='42501';
 END IF;
 IF octet_length(p_issuer) IS DISTINCT FROM 32 OR octet_length(p_subject) IS DISTINCT FROM 32
 OR octet_length(p_client) IS DISTINCT FROM 32 OR octet_length(p_token_id) IS DISTINCT FROM 32 THEN
  RAISE EXCEPTION 'invalid token guard' USING ERRCODE='22023';
 END IF;
 PERFORM pg_catalog.pg_advisory_xact_lock_shared(pg_catalog.hashtextextended(encode(p_issuer||p_subject||p_client||p_token_id,'hex'),7194057));
 IF public.novu_rust_operator_token_revoked(p_issuer,p_subject,p_client,p_token_id) THEN
  RAISE EXCEPTION 'operator token revoked' USING ERRCODE='42501';
 END IF;
 RETURN true;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_token_guard(bytea,bytea,bytea,bytea) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_operator_token_revocation_lock()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_old bigint; v_new bigint;
BEGIN
 IF TG_OP <> 'INSERT' THEN v_old:=pg_catalog.hashtextextended(encode(OLD.issuer_hash||OLD.subject_hash||OLD.client_hash||OLD.token_id_hash,'hex'),7194057); END IF;
 IF TG_OP <> 'DELETE' THEN v_new:=pg_catalog.hashtextextended(encode(NEW.issuer_hash||NEW.subject_hash||NEW.client_hash||NEW.token_id_hash,'hex'),7194057); END IF;
 IF v_old IS NOT NULL AND v_new IS NOT NULL AND v_old<>v_new THEN
  PERFORM pg_catalog.pg_advisory_xact_lock(least(v_old,v_new));
  PERFORM pg_catalog.pg_advisory_xact_lock(greatest(v_old,v_new));
 ELSE
  PERFORM pg_catalog.pg_advisory_xact_lock(coalesce(v_old,v_new));
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_token_revocation_lock() FROM PUBLIC;
DROP TRIGGER IF EXISTS novu_rust_operator_token_revocation_lock ON public.novu_rust_operator_token_revocations;
CREATE TRIGGER novu_rust_operator_token_revocation_lock BEFORE INSERT OR UPDATE OR DELETE ON public.novu_rust_operator_token_revocations
 FOR EACH ROW EXECUTE FUNCTION public.novu_rust_operator_token_revocation_lock();
