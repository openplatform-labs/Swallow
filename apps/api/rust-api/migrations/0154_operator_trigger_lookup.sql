-- Independent per-identity permission; no existing operator is implicitly granted access.
CREATE TABLE public.novu_rust_operator_jwt_trigger_lookup_grants (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
REVOKE ALL ON public.novu_rust_operator_jwt_trigger_lookup_grants FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_trigger_lookup(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_transaction text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb;
BEGIN
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_trigger_lookup_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF p_transaction IS NULL OR octet_length(p_transaction) NOT BETWEEN 1 AND 512 OR p_transaction ~ '[[:cntrl:]]' THEN RAISE EXCEPTION 'invalid transaction' USING ERRCODE='22023'; END IF;
 SELECT jsonb_build_object('transactionId',p_transaction,
  'recorded',EXISTS(SELECT 1 FROM public.novu_rust_trigger_transactions t WHERE t.environment_id=p_env AND t.transaction_id=p_transaction),
  'jobCount',count(*),'job',CASE WHEN count(*)=1 THEN (jsonb_agg(jsonb_build_object('jobId',x.id,'jobStatus',x.status))->0) ELSE NULL END)
 INTO result FROM (SELECT id,status FROM public.novu_queue_jobs WHERE environment_id=p_env AND kind IN ('experimental_static_in_app','experimental_static_in_app_attachments_v1') AND md5(payload->>'transactionId')=md5(p_transaction) AND payload->>'transactionId'=p_transaction ORDER BY id LIMIT 2) x;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_trigger_lookup(text,text,text,text,bigint,text) FROM PUBLIC;
