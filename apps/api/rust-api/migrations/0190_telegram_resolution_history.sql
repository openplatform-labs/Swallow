CREATE TABLE public.novu_rust_operator_jwt_telegram_resolution_history_viewers (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
REVOKE ALL ON public.novu_rust_operator_jwt_telegram_resolution_history_viewers FROM PUBLIC;
CREATE UNIQUE INDEX novu_rust_jwt_telegram_resolution_worker_lookup_idx ON public.novu_rust_operator_jwt_telegram_resolution_requests(environment_id,worker_request_id);
CREATE FUNCTION public.novu_rust_operator_jwt_telegram_resolution_history(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_job uuid,p_after uuid) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb; entries jsonb;
BEGIN
 IF p_job IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_telegram_resolution_history_viewers v WHERE v.issuer=p_issuer AND v.subject=p_subject AND v.client_id=p_client AND v.environment_id=p_env FOR SHARE OF v;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 result:=public.novu_rust_telegram_resolution_history(p_env,p_job,p_after);
 SELECT COALESCE(jsonb_agg(item || jsonb_build_object('operator',CASE WHEN a.worker_request_id IS NULL THEN NULL ELSE jsonb_build_object('issuer',a.issuer,'subject',a.subject,'clientId',a.client_id,'requestId',a.request_id) END) ORDER BY ord),'[]'::jsonb) INTO entries
 FROM jsonb_array_elements(result->'data') WITH ORDINALITY AS e(item,ord)
 LEFT JOIN public.novu_rust_operator_jwt_telegram_resolution_requests a ON a.environment_id=p_env AND a.worker_request_id=(item->>'requestId')::uuid;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_set(result,'{data}',entries);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_telegram_resolution_history(text,text,text,text,bigint,uuid,uuid) FROM PUBLIC;
