-- Dedicated delegated administration: existing lookup-grant administrators gain no rights.
CREATE TABLE public.novu_rust_operator_jwt_execution_grant_admins (
 issuer text NOT NULL, subject text NOT NULL, client_id text NOT NULL, environment_id text NOT NULL,
 can_read boolean NOT NULL DEFAULT false, can_write boolean NOT NULL DEFAULT false,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
CREATE TABLE public.novu_rust_operator_jwt_execution_grant_changes (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,request_id uuid NOT NULL,
 target_issuer text NOT NULL,target_subject text NOT NULL,target_client text NOT NULL,workflow text NOT NULL,
 expected jsonb NOT NULL,desired jsonb NOT NULL,reason text NOT NULL,changed boolean NOT NULL,
 database_principal name NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(issuer,subject,client_id,environment_id,request_id)
);
REVOKE ALL ON public.novu_rust_operator_jwt_execution_grant_admins,public.novu_rust_operator_jwt_execution_grant_changes FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_execution_grant_access(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_write boolean)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_write IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_execution_grant_admins WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND CASE WHEN p_write THEN can_write ELSE can_read END FOR SHARE;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_execution_grant_access(text,text,text,text,bigint,boolean) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_execution_state_valid(v jsonb) RETURNS boolean LANGUAGE sql IMMUTABLE SET search_path=pg_catalog AS $$
 SELECT COALESCE(jsonb_typeof(v)='object' AND (SELECT count(*)=2 FROM jsonb_object_keys(CASE WHEN jsonb_typeof(v)='object' THEN v ELSE '{}'::jsonb END)) AND jsonb_typeof(v->'granted')='boolean' AND (v->'workflowInternalId'='null'::jsonb OR (jsonb_typeof(v->'workflowInternalId')='string' AND octet_length(v->>'workflowInternalId') BETWEEN 1 AND 512 AND (v->>'workflowInternalId') !~ '[[:cntrl:]]')) AND (v->'granted'='true'::jsonb OR v->'workflowInternalId'='null'::jsonb),false)
$$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_execution_state_valid(jsonb) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_execution_grant_read(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_target_issuer text,p_target_subject text,p_target_client text,p_workflow text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE target_exists boolean; workflow_exists boolean; state jsonb;
BEGIN
 IF p_target_issuer IS NULL OR octet_length(p_target_issuer) NOT BETWEEN 1 AND 2048 OR p_target_subject IS NULL OR octet_length(p_target_subject) NOT BETWEEN 1 AND 1024 OR p_target_client IS NULL OR octet_length(p_target_client) NOT BETWEEN 1 AND 1024 OR p_workflow IS NULL OR octet_length(p_workflow) NOT BETWEEN 1 AND 512 OR p_workflow ~ '[[:cntrl:]]' THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_execution_grant_access(p_issuer,p_subject,p_client,p_env,p_exp,false);
 SELECT EXISTS(SELECT 1 FROM public.novu_rust_operator_jwt_identities WHERE issuer=p_target_issuer AND subject=p_target_subject AND client_id=p_target_client AND environment_id=p_env), EXISTS(SELECT 1 FROM public.novu_rust_workflows WHERE environment_id=p_env AND identifier=p_workflow) INTO target_exists,workflow_exists;
 SELECT jsonb_build_object('granted',true,'workflowInternalId',workflow_internal_id) INTO state FROM public.novu_rust_operator_jwt_trigger_execute_grants WHERE issuer=p_target_issuer AND subject=p_target_subject AND client_id=p_target_client AND environment_id=p_env AND workflow_identifier=p_workflow;
 IF state IS NULL THEN state:=jsonb_build_object('granted',false,'workflowInternalId',NULL); END IF;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('targetExists',target_exists,'workflowExists',workflow_exists,'state',state);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_execution_grant_read(text,text,text,text,bigint,text,text,text,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_execution_grant_change(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid,p_target_issuer text,p_target_subject text,p_target_client text,p_workflow text,p_expected jsonb,p_desired jsonb,p_reason text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_execution_grant_changes%ROWTYPE; current_state jsonb; current_id text; changed boolean;
BEGIN
 IF p_request IS NULL OR p_target_issuer IS NULL OR octet_length(p_target_issuer) NOT BETWEEN 1 AND 2048 OR p_target_subject IS NULL OR octet_length(p_target_subject) NOT BETWEEN 1 AND 1024 OR p_target_client IS NULL OR octet_length(p_target_client) NOT BETWEEN 1 AND 1024 OR p_workflow IS NULL OR octet_length(p_workflow) NOT BETWEEN 1 AND 512 OR p_workflow ~ '[[:cntrl:]]' OR NOT public.novu_rust_operator_execution_state_valid(p_expected) OR NOT public.novu_rust_operator_execution_state_valid(p_desired) OR p_reason IS NULL OR octet_length(btrim(p_reason)) NOT BETWEEN 1 AND 1000 THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_execution_grant_access(p_issuer,p_subject,p_client,p_env,p_exp,true);
 IF (p_issuer,p_subject)=(p_target_issuer,p_target_subject) THEN RAISE EXCEPTION 'self change denied' USING ERRCODE='42501'; END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended(jsonb_build_array('execution-grant',p_issuer,p_subject,p_client,p_env,p_request)::text,0));
 SELECT * INTO r FROM public.novu_rust_operator_jwt_execution_grant_changes WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
 IF FOUND THEN
  IF (r.target_issuer,r.target_subject,r.target_client,r.workflow,r.expected,r.desired,r.reason) IS DISTINCT FROM (p_target_issuer,p_target_subject,p_target_client,p_workflow,p_expected,p_desired,p_reason) THEN RAISE EXCEPTION 'request conflict' USING ERRCODE='P4090'; END IF;
  IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
  RETURN jsonb_build_object('changed',r.changed,'replayed',true,'requestId',p_request);
 END IF;
 PERFORM 1 FROM public.novu_rust_operator_jwt_identities WHERE issuer=p_target_issuer AND subject=p_target_subject AND client_id=p_target_client AND environment_id=p_env FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'target missing' USING ERRCODE='22023'; END IF;
 IF (p_desired->>'granted')::boolean THEN
  SELECT inbox_metadata->>'id' INTO current_id FROM public.novu_rust_workflows WHERE environment_id=p_env AND identifier=p_workflow FOR SHARE;
  IF NOT FOUND OR (p_desired->>'workflowInternalId' IS NOT NULL AND current_id IS DISTINCT FROM p_desired->>'workflowInternalId') THEN RAISE EXCEPTION 'workflow missing or generation changed' USING ERRCODE='P4090'; END IF;
 END IF;
 SELECT jsonb_build_object('granted',true,'workflowInternalId',workflow_internal_id) INTO current_state FROM public.novu_rust_operator_jwt_trigger_execute_grants WHERE issuer=p_target_issuer AND subject=p_target_subject AND client_id=p_target_client AND environment_id=p_env AND workflow_identifier=p_workflow FOR UPDATE;
 IF current_state IS NULL THEN current_state:=jsonb_build_object('granted',false,'workflowInternalId',NULL); END IF;
 IF current_state IS DISTINCT FROM p_expected THEN RAISE EXCEPTION 'stale grant' USING ERRCODE='P4090'; END IF;
 changed:=current_state IS DISTINCT FROM p_desired;
 IF changed THEN
  IF (p_desired->>'granted')::boolean THEN INSERT INTO public.novu_rust_operator_jwt_trigger_execute_grants(issuer,subject,client_id,environment_id,workflow_identifier,workflow_internal_id) VALUES(p_target_issuer,p_target_subject,p_target_client,p_env,p_workflow,p_desired->>'workflowInternalId') ON CONFLICT(issuer,subject,client_id,environment_id,workflow_identifier) DO UPDATE SET workflow_internal_id=EXCLUDED.workflow_internal_id;
  ELSE DELETE FROM public.novu_rust_operator_jwt_trigger_execute_grants WHERE issuer=p_target_issuer AND subject=p_target_subject AND client_id=p_target_client AND environment_id=p_env AND workflow_identifier=p_workflow; END IF;
 END IF;
 INSERT INTO public.novu_rust_operator_jwt_execution_grant_changes(issuer,subject,client_id,environment_id,request_id,target_issuer,target_subject,target_client,workflow,expected,desired,reason,changed,database_principal) VALUES(p_issuer,p_subject,p_client,p_env,p_request,p_target_issuer,p_target_subject,p_target_client,p_workflow,p_expected,p_desired,p_reason,changed,session_user);
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('changed',changed,'replayed',false,'requestId',p_request);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_execution_grant_change(text,text,text,text,bigint,uuid,text,text,text,text,jsonb,jsonb,text) FROM PUBLIC;
