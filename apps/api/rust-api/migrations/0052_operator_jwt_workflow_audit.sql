-- Reviewing workflow audit is independent from reading or writing a template.
CREATE TABLE public.novu_rust_operator_jwt_workflow_audit_grants (
 issuer text NOT NULL, subject text NOT NULL, client_id text NOT NULL, environment_id text NOT NULL,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id)
  REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
REVOKE ALL ON public.novu_rust_operator_jwt_workflow_audit_grants FROM PUBLIC;
CREATE INDEX novu_rust_operator_jwt_workflow_audit_lookup_idx ON public.novu_rust_operator_jwt_workflow_audit(environment_id,workflow_identifier,resource,id);
CREATE FUNCTION public.novu_rust_operator_jwt_workflow_audit_read(
 p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_workflow text,p_resource text,p_after uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_result jsonb;
BEGIN
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 IF p_workflow IS NULL OR octet_length(p_workflow) NOT BETWEEN 1 AND 512 OR p_resource IS NULL OR p_resource NOT IN ('email','sms','push','slack','email-provider') THEN
  RAISE EXCEPTION 'invalid request' USING ERRCODE='22023';
 END IF;
 PERFORM 1 FROM public.novu_rust_operator_jwt_workflow_audit_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 WITH candidates AS MATERIALIZED (
  SELECT a.id,a.workflow_identifier,a.resource,a.issuer,a.subject,a.client_id,a.database_principal,a.reason,a.previous_active,a.new_active,a.previous_title_bytes,a.new_title_bytes,a.previous_content_bytes,a.new_content_bytes,a.previous_provider,a.new_provider,a.recorded_at
  FROM public.novu_rust_operator_jwt_workflow_audit a WHERE a.environment_id=p_env AND a.workflow_identifier=p_workflow AND a.resource=p_resource AND (p_after IS NULL OR a.id>p_after) ORDER BY a.id LIMIT 51
 ), page AS MATERIALIZED (SELECT * FROM candidates ORDER BY id LIMIT 50)
 SELECT jsonb_build_object('data',coalesce((SELECT jsonb_agg(jsonb_build_object('id',id,'workflowIdentifier',workflow_identifier,'resource',resource,'issuer',issuer,'subject',subject,'clientId',client_id,'databasePrincipal',database_principal,'reason',reason,'previousActive',previous_active,'newActive',new_active,'previousTitleBytes',previous_title_bytes,'newTitleBytes',new_title_bytes,'previousContentBytes',previous_content_bytes,'newContentBytes',new_content_bytes,'previousProvider',previous_provider,'newProvider',new_provider,'recordedAt',recorded_at::text) ORDER BY id) FROM page),'[]'::jsonb),
 'hasMore',(SELECT count(*)>50 FROM candidates),'next',CASE WHEN (SELECT count(*)>50 FROM candidates) THEN (SELECT id::text FROM page ORDER BY id DESC LIMIT 1) ELSE NULL END)
 INTO v_result;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN v_result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_workflow_audit_read(text,text,text,text,bigint,text,text,uuid) FROM PUBLIC;
