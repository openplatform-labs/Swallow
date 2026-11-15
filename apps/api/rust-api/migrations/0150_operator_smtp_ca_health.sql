-- Independent JWT permission; the API DB login also needs alert_view.
CREATE TABLE public.novu_rust_operator_jwt_smtp_ca_health_grants (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,
 can_health boolean NOT NULL DEFAULT false,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
REVOKE ALL ON public.novu_rust_operator_jwt_smtp_ca_health_grants FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_smtp_ca_health(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_max_age integer,p_minimum integer)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE permitted boolean;result jsonb;
BEGIN
 IF p_max_age IS NULL OR p_max_age NOT BETWEEN 1 AND 86400 OR p_minimum IS NULL OR p_minimum NOT BETWEEN 1 AND 1000 THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 SELECT can_health INTO permitted FROM public.novu_rust_operator_jwt_smtp_ca_health_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT coalesce(permitted,false) OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 SELECT jsonb_build_object('healthy',healthy,'unhealthy',unhealthy,'stale',stale,'minimumHealthy',p_minimum,'needsAttention',healthy<p_minimum OR unhealthy>0) INTO result FROM public.novu_rust_smtp_ca_health_check(p_env,p_max_age);
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_smtp_ca_health(text,text,text,text,bigint,integer,integer) FROM PUBLIC;
