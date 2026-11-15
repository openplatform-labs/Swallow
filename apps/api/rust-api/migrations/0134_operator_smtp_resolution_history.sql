-- Viewing resolution history requires a dedicated JWT grant and DB viewer grant.
CREATE TABLE public.novu_rust_operator_jwt_smtp_resolution_history_viewers (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
REVOKE ALL ON public.novu_rust_operator_jwt_smtp_resolution_history_viewers FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_smtp_resolution_history(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_job uuid,p_before_actor oid,p_before_request uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_result jsonb; v_data jsonb;
BEGIN
 IF p_job IS NULL OR ((p_before_actor IS NULL)<>(p_before_request IS NULL)) THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_smtp_resolution_history_viewers v WHERE v.issuer=p_issuer AND v.subject=p_subject AND v.client_id=p_client AND v.environment_id=p_env FOR SHARE OF v;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 v_result:=public.novu_rust_smtp_resolution_history(p_env,p_job,p_before_actor,p_before_request);
 SELECT coalesce(jsonb_agg(jsonb_set(item,'{failureRevision}',to_jsonb(item->>'failureRevision')) ORDER BY ord),'[]'::jsonb) INTO v_data FROM jsonb_array_elements(v_result->'data') WITH ORDINALITY AS e(item,ord);
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_set(v_result,'{data}',v_data);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_smtp_resolution_history(text,text,text,text,bigint,uuid,oid,uuid) FROM PUBLIC;
