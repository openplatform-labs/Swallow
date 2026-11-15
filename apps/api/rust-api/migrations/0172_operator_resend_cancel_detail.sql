-- Detail includes sensitive provider and audit data; list access alone is insufficient.
CREATE TABLE public.novu_rust_operator_jwt_resend_cancel_detail_grants (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
REVOKE ALL ON public.novu_rust_operator_jwt_resend_cancel_detail_grants FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_resend_cancel_detail(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_request jsonb;v_resolution jsonb;
BEGIN
 IF p_request IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_resend_cancel_detail_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 v_request:=public.novu_rust_resend_cancel_get(p_env,p_request);
 SELECT jsonb_build_object('resolutionId',r.resolution_id,'decision',r.decision,'databasePrincipal',r.database_principal,'reason',r.reason,'recordedAt',r.recorded_at::text) INTO v_resolution FROM public.novu_rust_resend_cancel_resolutions r WHERE r.environment_id=p_env AND r.request_id=p_request;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('request',v_request,'resolution',v_resolution);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_resend_cancel_detail(text,text,text,text,bigint,uuid) FROM PUBLIC;
