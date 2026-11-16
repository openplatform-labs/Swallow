-- Aggregate health has its own JWT grant; the API database login also needs alert_view.
CREATE TABLE public.novu_rust_operator_jwt_telegram_health_grants (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
REVOKE ALL ON public.novu_rust_operator_jwt_telegram_health_grants FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_telegram_health(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb;
BEGIN
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_telegram_health_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 SELECT jsonb_build_object('unknownAttempts',unknown_attempts,'rejectedAttempts',rejected_attempts,'strandedDispatches',stranded_dispatches,'preDispatchFailures',pre_dispatch_failures) INTO result FROM public.novu_rust_telegram_health(p_env);
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_telegram_health(text,text,text,text,bigint) FROM PUBLIC;
