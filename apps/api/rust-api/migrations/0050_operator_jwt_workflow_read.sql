-- Workflow access is independent for each identity, environment and resource.
CREATE TABLE public.novu_rust_operator_jwt_workflow_grants (
 issuer text NOT NULL, subject text NOT NULL, client_id text NOT NULL, environment_id text NOT NULL,
 resource text NOT NULL CHECK (resource IN ('email','sms','push','slack','email-provider')),
 permission text NOT NULL CHECK (permission IN ('read','write')),
 PRIMARY KEY(issuer,subject,client_id,environment_id,resource,permission),
 FOREIGN KEY(issuer,subject,client_id,environment_id)
  REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
REVOKE ALL ON public.novu_rust_operator_jwt_workflow_grants FROM PUBLIC;

CREATE FUNCTION public.novu_rust_operator_jwt_workflow_read(
 p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_workflow text,p_resource text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_result jsonb;
BEGIN
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 IF p_resource IS NULL OR p_resource NOT IN ('email','sms','push','slack','email-provider') OR p_workflow IS NULL OR octet_length(p_workflow) NOT BETWEEN 1 AND 512 THEN
  RAISE EXCEPTION 'invalid request' USING ERRCODE='22023';
 END IF;
 PERFORM 1 FROM public.novu_rust_operator_jwt_workflow_grants g
  WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env AND g.resource=p_resource AND g.permission='read' FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN
  RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501';
 END IF;
 SELECT CASE p_resource
  WHEN 'email' THEN jsonb_build_object('workflowIdentifier',w.identifier,'channel','email','title',w.email_subject,'content',w.email_html,'active',w.email_step_active)
  WHEN 'sms' THEN jsonb_build_object('workflowIdentifier',w.identifier,'channel','sms','title',NULL,'content',w.sms_content,'active',w.sms_step_active)
  WHEN 'push' THEN jsonb_build_object('workflowIdentifier',w.identifier,'channel','push','title',w.push_title,'content',w.push_content,'active',w.push_step_active)
  WHEN 'slack' THEN jsonb_build_object('workflowIdentifier',w.identifier,'content',w.chat_content,'active',w.chat_step_active)
  WHEN 'email-provider' THEN jsonb_build_object('workflowIdentifier',w.identifier,'provider',w.email_provider)
 END INTO v_result FROM public.novu_rust_workflows w WHERE w.environment_id=p_env AND w.identifier=p_workflow;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF v_result IS NULL THEN RAISE EXCEPTION 'workflow not found' USING ERRCODE='22023'; END IF;
 RETURN jsonb_build_object('data',v_result);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_workflow_read(text,text,text,text,bigint,text,text) FROM PUBLIC;
