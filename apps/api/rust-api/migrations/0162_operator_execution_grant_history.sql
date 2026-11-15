-- Stable page key for existing and future change records. UUID order is not time order.
ALTER TABLE public.novu_rust_operator_jwt_execution_grant_changes ADD COLUMN id uuid NOT NULL DEFAULT gen_random_uuid();
CREATE UNIQUE INDEX novu_rust_execution_grant_changes_id_idx ON public.novu_rust_operator_jwt_execution_grant_changes(id);
CREATE INDEX novu_rust_execution_grant_changes_env_page_idx ON public.novu_rust_operator_jwt_execution_grant_changes(environment_id,id);
CREATE TABLE public.novu_rust_operator_jwt_execution_grant_history_viewers (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
REVOKE ALL ON public.novu_rust_operator_jwt_execution_grant_history_viewers FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_execution_grant_request(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_execution_grant_changes%ROWTYPE;
BEGIN
 IF p_request IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_execution_grant_access(p_issuer,p_subject,p_client,p_env,p_exp,true);
 SELECT * INTO r FROM public.novu_rust_operator_jwt_execution_grant_changes WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF NOT FOUND THEN RETURN jsonb_build_object('found',false); END IF;
 RETURN jsonb_build_object('found',true,'requestId',r.request_id,'targetIssuer',r.target_issuer,'targetSubject',r.target_subject,'targetClientId',r.target_client,'workflow',r.workflow,'expected',r.expected,'desired',r.desired,'reason',r.reason,'changed',r.changed,'createdAt',r.created_at);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_execution_grant_request(text,text,text,text,bigint,uuid) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_execution_grant_history(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_after uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb;
BEGIN
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_execution_grant_history_viewers WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env FOR SHARE;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 WITH candidates AS MATERIALIZED (SELECT * FROM public.novu_rust_operator_jwt_execution_grant_changes WHERE environment_id=p_env AND (p_after IS NULL OR id>p_after) ORDER BY id LIMIT 51), page AS MATERIALIZED (SELECT * FROM candidates ORDER BY id LIMIT 50)
 SELECT jsonb_build_object('data',coalesce((SELECT jsonb_agg(jsonb_build_object('id',id,'actorIssuer',issuer,'actorSubject',subject,'actorClientId',client_id,'databasePrincipal',database_principal,'requestId',request_id,'targetIssuer',target_issuer,'targetSubject',target_subject,'targetClientId',target_client,'workflow',workflow,'expected',expected,'desired',desired,'reason',reason,'changed',changed,'recordedAt',created_at::text) ORDER BY id) FROM page),'[]'::jsonb),'hasMore',(SELECT count(*)>50 FROM candidates),'next',CASE WHEN (SELECT count(*)>50 FROM candidates) THEN (SELECT id::text FROM page ORDER BY id DESC LIMIT 1) ELSE NULL END) INTO result;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_execution_grant_history(text,text,text,text,bigint,uuid) FROM PUBLIC;
