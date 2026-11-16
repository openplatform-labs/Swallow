-- Existing independent Telegram history viewers can inspect pre-dispatch actions.
CREATE FUNCTION public.novu_rust_operator_jwt_telegram_predispatch_history(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_job uuid,p_after uuid,p_action text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb; entries jsonb;
BEGIN
 IF p_job IS NULL OR (p_after IS NULL)<>(p_action IS NULL) OR (p_action IS NOT NULL AND p_action NOT IN ('cancel','retry')) THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_telegram_resolution_history_viewers v WHERE v.issuer=p_issuer AND v.subject=p_subject AND v.client_id=p_client AND v.environment_id=p_env FOR SHARE OF v;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 result:=public.novu_rust_telegram_predispatch_history(p_env,p_job,p_after,p_action);
 SELECT COALESCE(jsonb_agg(item || jsonb_build_object('operator',CASE
   WHEN item->>'action'='retry' AND t.worker_request_id IS NOT NULL THEN jsonb_build_object('issuer',t.issuer,'subject',t.subject,'clientId',t.client_id,'requestId',t.request_id)
   WHEN item->>'action'='cancel' AND c.worker_request_id IS NOT NULL THEN jsonb_build_object('issuer',c.issuer,'subject',c.subject,'clientId',c.client_id,'requestId',c.request_id)
   ELSE NULL END) ORDER BY ord),'[]'::jsonb) INTO entries
 FROM jsonb_array_elements(result->'data') WITH ORDINALITY e(item,ord)
 LEFT JOIN public.novu_rust_operator_jwt_telegram_predispatch_requests t ON item->>'action'='retry' AND t.environment_id=p_env AND t.job_id=p_job AND t.worker_request_id=(item->>'requestId')::uuid
 LEFT JOIN public.novu_rust_operator_jwt_telegram_predispatch_cancellations c ON item->>'action'='cancel' AND c.environment_id=p_env AND c.job_id=p_job AND c.worker_request_id=(item->>'requestId')::uuid;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_set(result,'{data}',entries);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_telegram_predispatch_history(text,text,text,text,bigint,uuid,uuid,text) FROM PUBLIC;
