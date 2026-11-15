-- Listing request IDs is independently granted per JWT identity and environment.
CREATE TABLE public.novu_rust_operator_jwt_resend_cancel_list_grants (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
REVOKE ALL ON public.novu_rust_operator_jwt_resend_cancel_list_grants FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_resend_cancel_list(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_grace integer,p_after uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb;
BEGIN
 IF p_grace IS NULL OR p_grace NOT BETWEEN 1 AND 86400 THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_resend_cancel_list_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 WITH page AS MATERIALIZED (SELECT * FROM public.novu_rust_resend_cancel_list(p_env,p_grace,p_after)), numbered AS (SELECT *,row_number() OVER (ORDER BY request_id) AS n FROM page)
 SELECT jsonb_build_object('data',COALESCE(jsonb_agg(jsonb_build_object('requestId',request_id,'jobId',job_id,'createdAt',created_at,'overdue',overdue) ORDER BY request_id) FILTER(WHERE n<=50),'[]'::jsonb),'next',CASE WHEN count(*)>50 THEN (max(request_id::text) FILTER(WHERE n=50)) ELSE NULL END) INTO result FROM numbered;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_resend_cancel_list(text,text,text,text,bigint,integer,uuid) FROM PUBLIC;
