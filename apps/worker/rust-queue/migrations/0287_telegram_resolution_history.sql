-- Independently authorized DB viewers; authority is tied to the LOGIN OID.
CREATE TABLE public.novu_rust_telegram_history_viewers (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id) ON DELETE CASCADE,
 database_principal name NOT NULL,principal_oid oid NOT NULL,
 PRIMARY KEY(environment_id,database_principal)
);
REVOKE ALL ON public.novu_rust_telegram_history_viewers FROM PUBLIC;
CREATE INDEX novu_rust_telegram_resolution_history_idx ON public.novu_rust_telegram_resolutions(environment_id,job_id,request_id);
CREATE FUNCTION public.novu_rust_telegram_resolution_history(p_env text,p_job uuid,p_after uuid) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb;
BEGIN
 IF p_env IS NULL OR length(btrim(p_env))=0 OR p_job IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM 1 FROM public.novu_rust_telegram_history_viewers v JOIN pg_catalog.pg_roles r ON r.rolname=v.database_principal AND r.oid=v.principal_oid AND r.rolcanlogin WHERE v.environment_id=p_env AND v.database_principal=session_user FOR SHARE OF v;
 IF NOT FOUND THEN RAISE EXCEPTION 'history access denied' USING ERRCODE='42501'; END IF;
 WITH candidates AS MATERIALIZED (SELECT * FROM public.novu_rust_telegram_resolutions r WHERE r.environment_id=p_env AND r.job_id=p_job AND (p_after IS NULL OR r.request_id>p_after) ORDER BY r.request_id LIMIT 51),page AS MATERIALIZED (SELECT * FROM candidates ORDER BY request_id LIMIT 50)
 SELECT jsonb_build_object('data',COALESCE((SELECT jsonb_agg(jsonb_build_object('requestId',request_id,'jobId',job_id,'attemptId',attempt_id,'previousState',previous_state,'httpStatus',previous_http_status,'previousUpdatedAt',previous_updated_at,'decision',decision,'reason',reason,'databasePrincipal',database_principal,'principalOid',principal_oid::text,'createdAt',created_at) ORDER BY request_id) FROM page),'[]'::jsonb),'hasMore',(SELECT count(*)>50 FROM candidates),'next',CASE WHEN (SELECT count(*)>50 FROM candidates) THEN (SELECT request_id FROM page ORDER BY request_id DESC LIMIT 1) ELSE NULL END) INTO result;
 RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_telegram_resolution_history(text,uuid,uuid) FROM PUBLIC;
