-- Separately scoped JWT workflow writes; audit stores metadata, not templates.
CREATE TABLE public.novu_rust_operator_jwt_workflow_audit (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), environment_id text NOT NULL, workflow_identifier text NOT NULL,
 resource text NOT NULL, issuer text NOT NULL, subject text NOT NULL, client_id text NOT NULL,
 database_principal name NOT NULL, reason text NOT NULL,
 previous_active boolean, new_active boolean, previous_title_bytes integer, new_title_bytes integer,
 previous_content_bytes integer, new_content_bytes integer, previous_provider text, new_provider text,
 recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX novu_rust_operator_jwt_workflow_audit_env_idx ON public.novu_rust_operator_jwt_workflow_audit(environment_id,recorded_at DESC,id);
REVOKE ALL ON public.novu_rust_operator_jwt_workflow_audit FROM PUBLIC;

CREATE FUNCTION public.novu_rust_operator_jwt_workflow_value_valid(p_resource text,p_value jsonb)
RETURNS boolean LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
DECLARE n integer;
BEGIN
 IF p_value IS NULL OR jsonb_typeof(p_value)<>'object' THEN RETURN false; END IF;
 SELECT count(*) INTO n FROM jsonb_object_keys(p_value);
 IF p_resource='email-provider' THEN
  RETURN n=1 AND COALESCE(jsonb_typeof(p_value->'provider')='string' AND p_value->>'provider' IN ('sendgrid','resend'),false);
 END IF;
 IF p_resource NOT IN ('email','sms','push','slack') THEN RETURN false; END IF;
 IF NOT (p_value ? 'content' AND p_value ? 'active') OR jsonb_typeof(p_value->'active')<>'boolean' OR jsonb_typeof(p_value->'content') NOT IN ('string','null') OR COALESCE(octet_length(p_value->>'content'),0)>40000 THEN RETURN false; END IF;
 IF p_resource='slack' THEN RETURN n=2; END IF;
 IF n<>3 OR NOT p_value ? 'title' OR jsonb_typeof(p_value->'title') NOT IN ('string','null') OR COALESCE(octet_length(p_value->>'title'),0)>40000 THEN RETURN false; END IF;
 IF p_resource='sms' THEN RETURN jsonb_typeof(p_value->'title')='null'; END IF;
 RETURN (jsonb_typeof(p_value->'title')='null')=(jsonb_typeof(p_value->'content')='null');
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_workflow_value_valid(text,jsonb) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_operator_jwt_workflow_write(
 p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_workflow text,p_resource text,p_expected jsonb,p_desired jsonb,p_reason text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE w public.novu_rust_workflows%ROWTYPE; v_current jsonb; v_result jsonb;
BEGIN
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 IF p_resource IS NULL OR p_workflow IS NULL OR octet_length(p_workflow) NOT BETWEEN 1 AND 512 OR p_reason IS NULL OR octet_length(btrim(p_reason)) NOT BETWEEN 1 AND 1000 OR NOT public.novu_rust_operator_jwt_workflow_value_valid(p_resource,p_expected) OR NOT public.novu_rust_operator_jwt_workflow_value_valid(p_resource,p_desired) THEN
  RAISE EXCEPTION 'invalid request' USING ERRCODE='22023';
 END IF;
 PERFORM 1 FROM public.novu_rust_operator_jwt_workflow_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env AND g.resource=p_resource AND g.permission='write' FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 SELECT * INTO w FROM public.novu_rust_workflows WHERE environment_id=p_env AND identifier=p_workflow FOR UPDATE;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF NOT FOUND THEN RAISE EXCEPTION 'workflow not found' USING ERRCODE='22023'; END IF;
 v_current := CASE p_resource
  WHEN 'email' THEN jsonb_build_object('title',w.email_subject,'content',w.email_html,'active',w.email_step_active)
  WHEN 'sms' THEN jsonb_build_object('title',NULL,'content',w.sms_content,'active',w.sms_step_active)
  WHEN 'push' THEN jsonb_build_object('title',w.push_title,'content',w.push_content,'active',w.push_step_active)
  WHEN 'slack' THEN jsonb_build_object('content',w.chat_content,'active',w.chat_step_active)
  WHEN 'email-provider' THEN jsonb_build_object('provider',w.email_provider)
 END;
 IF v_current IS DISTINCT FROM p_expected THEN RAISE EXCEPTION 'stale workflow' USING ERRCODE='P4090'; END IF;
 CASE p_resource
  WHEN 'email' THEN UPDATE public.novu_rust_workflows SET email_subject=p_desired->>'title',email_html=p_desired->>'content',email_step_active=(p_desired->>'active')::boolean WHERE environment_id=p_env AND identifier=p_workflow;
  WHEN 'sms' THEN UPDATE public.novu_rust_workflows SET sms_content=p_desired->>'content',sms_step_active=(p_desired->>'active')::boolean WHERE environment_id=p_env AND identifier=p_workflow;
  WHEN 'push' THEN UPDATE public.novu_rust_workflows SET push_title=p_desired->>'title',push_content=p_desired->>'content',push_step_active=(p_desired->>'active')::boolean WHERE environment_id=p_env AND identifier=p_workflow;
  WHEN 'slack' THEN UPDATE public.novu_rust_workflows SET chat_content=p_desired->>'content',chat_step_active=(p_desired->>'active')::boolean WHERE environment_id=p_env AND identifier=p_workflow;
  WHEN 'email-provider' THEN UPDATE public.novu_rust_workflows SET email_provider=p_desired->>'provider' WHERE environment_id=p_env AND identifier=p_workflow;
 END CASE;
 INSERT INTO public.novu_rust_operator_jwt_workflow_audit(environment_id,workflow_identifier,resource,issuer,subject,client_id,database_principal,reason,previous_active,new_active,previous_title_bytes,new_title_bytes,previous_content_bytes,new_content_bytes,previous_provider,new_provider)
 VALUES(p_env,p_workflow,p_resource,p_issuer,p_subject,p_client,session_user,btrim(p_reason),(v_current->>'active')::boolean,(p_desired->>'active')::boolean,octet_length(v_current->>'title'),octet_length(p_desired->>'title'),octet_length(v_current->>'content'),octet_length(p_desired->>'content'),v_current->>'provider',p_desired->>'provider');
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 v_result:=jsonb_build_object('workflowIdentifier',p_workflow)||p_desired;
 IF p_resource IN ('email','sms','push') THEN v_result:=v_result||jsonb_build_object('channel',p_resource); END IF;
 RETURN jsonb_build_object('data',v_result);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_workflow_write(text,text,text,text,bigint,text,text,jsonb,jsonb,text) FROM PUBLIC;
