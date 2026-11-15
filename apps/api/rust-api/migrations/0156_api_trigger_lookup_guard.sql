-- Lock the caller's key and user for the complete lookup without granting the
-- API login UPDATE on the credential tables merely for SELECT FOR SHARE.
CREATE FUNCTION public.novu_rust_api_trigger_lookup(p_hash text,p_env text,p_transaction text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb;
BEGIN
 IF p_hash IS NULL OR p_hash !~ '^[0-9a-f]{64}$' OR p_env IS NULL OR octet_length(p_env)=0 OR p_transaction IS NULL OR octet_length(p_transaction) NOT BETWEEN 1 AND 512 OR p_transaction ~ '[[:cntrl:]]' THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM 1 FROM public.novu_rust_api_keys k JOIN public.novu_rust_users u ON u.id=k.user_id WHERE k.key_hash=p_hash AND k.environment_id=p_env AND k.enabled FOR SHARE OF k,u;
 IF NOT FOUND THEN RETURN NULL; END IF;
 SELECT jsonb_build_object('transactionId',p_transaction,'recorded',EXISTS(SELECT 1 FROM public.novu_rust_trigger_transactions t WHERE t.environment_id=p_env AND t.transaction_id=p_transaction),'jobCount',count(*),'job',CASE WHEN count(*)=1 THEN (jsonb_agg(jsonb_build_object('jobId',x.id,'jobStatus',x.status))->0) ELSE NULL END)
 INTO result FROM (SELECT id,status FROM public.novu_queue_jobs WHERE environment_id=p_env AND kind IN ('experimental_static_in_app','experimental_static_in_app_attachments_v1') AND md5(payload->>'transactionId')=md5(p_transaction) AND payload->>'transactionId'=p_transaction ORDER BY id LIMIT 2) x;
 RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_api_trigger_lookup(text,text,text) FROM PUBLIC;
