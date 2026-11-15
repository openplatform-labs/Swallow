-- Audit rights are separate from channel grant administration.
CREATE TABLE public.novu_rust_operator_jwt_channel_access_grant_audit_scopes (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,
 resource text NOT NULL CHECK(resource IN ('resend','sendgrid','twilio','fcm','slack')),
 PRIMARY KEY(issuer,subject,client_id,environment_id,resource),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
REVOKE ALL ON public.novu_rust_operator_jwt_channel_access_grant_audit_scopes FROM PUBLIC;
CREATE INDEX novu_rust_channel_grant_audit_resource_page_idx ON public.novu_rust_operator_jwt_channel_access_grant_audit(environment_id,resource,id);
CREATE FUNCTION public.novu_rust_operator_jwt_channel_access_grant_audit_filtered(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_resource text,p_after uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_result jsonb;
BEGIN
 IF p_resource IS NULL OR p_resource NOT IN ('resend','sendgrid','twilio','fcm','slack') THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_channel_access_grant_audit_viewers g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND THEN
  PERFORM 1 FROM public.novu_rust_operator_jwt_channel_access_grant_audit_scopes g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env AND g.resource=p_resource FOR SHARE OF g;
  IF NOT FOUND THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 END IF;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 WITH candidates AS MATERIALIZED (
  SELECT id,actor_issuer,actor_subject,actor_client,database_principal,request_id,target_issuer,target_subject,target_client,resource,permission,previous,current,reason,recorded_at
  FROM public.novu_rust_operator_jwt_channel_access_grant_audit WHERE environment_id=p_env AND resource=p_resource AND (p_after IS NULL OR id>p_after) ORDER BY id LIMIT 51
 ), page AS MATERIALIZED (SELECT * FROM candidates ORDER BY id LIMIT 50)
 SELECT jsonb_build_object('data',coalesce((SELECT jsonb_agg(jsonb_build_object('id',id,'actorIssuer',actor_issuer,'actorSubject',actor_subject,'actorClientId',actor_client,'databasePrincipal',database_principal,'requestId',request_id,'targetIssuer',target_issuer,'targetSubject',target_subject,'targetClientId',target_client,'resource',resource,'permission',permission,'previous',previous,'current',current,'reason',reason,'recordedAt',recorded_at::text) ORDER BY id) FROM page),'[]'::jsonb),'hasMore',(SELECT count(*)>50 FROM candidates),'next',CASE WHEN (SELECT count(*)>50 FROM candidates) THEN (SELECT id::text FROM page ORDER BY id DESC LIMIT 1) ELSE NULL END) INTO v_result;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN v_result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_channel_access_grant_audit_filtered(text,text,text,text,bigint,text,uuid) FROM PUBLIC;
