-- Audit access is separate from the ability to delete observations.
ALTER TABLE public.novu_rust_operator_jwt_cancel_cred_retention_requests ADD COLUMN id uuid NOT NULL DEFAULT gen_random_uuid();
CREATE UNIQUE INDEX novu_rust_cancel_cred_retention_request_audit_idx ON public.novu_rust_operator_jwt_cancel_cred_retention_requests(environment_id,id);
CREATE TABLE public.novu_rust_cancel_cred_retention_audit_db_viewers (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id), database_principal name NOT NULL,principal_oid oid NOT NULL,
 PRIMARY KEY(environment_id,database_principal)
);
CREATE TABLE public.novu_rust_operator_jwt_cancel_cred_retention_audit_viewers (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
REVOKE ALL ON public.novu_rust_cancel_cred_retention_audit_db_viewers,public.novu_rust_operator_jwt_cancel_cred_retention_audit_viewers FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_cancel_cred_retention_audit(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_after uuid) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb;
BEGIN
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_cancel_cred_retention_audit_viewers g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 PERFORM 1 FROM public.novu_rust_cancel_cred_retention_audit_db_viewers g JOIN pg_catalog.pg_roles r ON r.rolname=g.database_principal AND r.oid=g.principal_oid AND r.rolcanlogin WHERE g.environment_id=p_env AND g.database_principal=session_user FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 WITH candidates AS MATERIALIZED (SELECT id,issuer,subject,client_id,request_id,older_than_seconds,batch_limit,reason,deleted_count,created_at FROM public.novu_rust_operator_jwt_cancel_cred_retention_requests WHERE environment_id=p_env AND (p_after IS NULL OR id>p_after) ORDER BY id LIMIT 51),
 page AS MATERIALIZED (SELECT * FROM candidates ORDER BY id LIMIT 50)
 SELECT jsonb_build_object('data',coalesce((SELECT jsonb_agg(jsonb_build_object('id',id,'actorIssuer',issuer,'actorSubject',subject,'actorClientId',client_id,'requestId',request_id,'olderThanSeconds',older_than_seconds,'batchLimit',batch_limit,'reason',reason,'deleted',deleted_count,'recordedAt',created_at::text) ORDER BY id) FROM page),'[]'::jsonb),'hasMore',(SELECT count(*)>50 FROM candidates),'next',CASE WHEN (SELECT count(*)>50 FROM candidates) THEN (SELECT id::text FROM page ORDER BY id DESC LIMIT 1) ELSE NULL END) INTO result;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_cancel_cred_retention_audit(text,text,text,text,bigint,uuid) FROM PUBLIC;
