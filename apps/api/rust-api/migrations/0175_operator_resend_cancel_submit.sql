-- A separate JWT right is required to request cancellation commands.
CREATE TABLE public.novu_rust_operator_jwt_resend_cancel_submit_grants (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,
 PRIMARY KEY(issuer,subject,client_id,environment_id),FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
REVOKE ALL ON public.novu_rust_operator_jwt_resend_cancel_submit_grants FROM PUBLIC;
CREATE OR REPLACE FUNCTION public.novu_rust_operator_jwt_resend_cancel_grant_input_valid(p_issuer text,p_subject text,p_client text,p_resource text,p_permission text)
RETURNS boolean LANGUAGE sql IMMUTABLE SET search_path=pg_catalog AS $$
 SELECT COALESCE(octet_length(p_issuer) BETWEEN 1 AND 2048 AND octet_length(p_subject) BETWEEN 1 AND 1024 AND octet_length(p_client) BETWEEN 1 AND 1024 AND (p_resource='resend-cancel' AND p_permission IN ('list','detail','confirmed','not_cancelled','submit')),false)
$$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_resend_cancel_grant_input_valid(text,text,text,text,text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_operator_jwt_resend_cancel_grant_read(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_target_issuer text,p_target_subject text,p_target_client text,p_resource text,p_permission text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_exists boolean; v_granted boolean;
BEGIN
 IF NOT public.novu_rust_operator_jwt_resend_cancel_grant_input_valid(p_target_issuer,p_target_subject,p_target_client,p_resource,p_permission) THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_resend_cancel_grant_admin_access(p_issuer,p_subject,p_client,p_env,p_exp,false);
 PERFORM 1 FROM public.novu_rust_operator_jwt_identities i WHERE i.issuer=p_target_issuer AND i.subject=p_target_subject AND i.client_id=p_target_client AND i.environment_id=p_env FOR SHARE OF i;
 v_exists:=FOUND;
 SELECT CASE p_permission WHEN 'submit' THEN EXISTS(SELECT 1 FROM public.novu_rust_operator_jwt_resend_cancel_submit_grants g WHERE g.issuer=p_target_issuer AND g.subject=p_target_subject AND g.client_id=p_target_client AND g.environment_id=p_env) WHEN 'list' THEN EXISTS(SELECT 1 FROM public.novu_rust_operator_jwt_resend_cancel_list_grants g WHERE g.issuer=p_target_issuer AND g.subject=p_target_subject AND g.client_id=p_target_client AND g.environment_id=p_env) WHEN 'detail' THEN EXISTS(SELECT 1 FROM public.novu_rust_operator_jwt_resend_cancel_detail_grants g WHERE g.issuer=p_target_issuer AND g.subject=p_target_subject AND g.client_id=p_target_client AND g.environment_id=p_env) ELSE EXISTS(SELECT 1 FROM public.novu_rust_operator_jwt_resend_cancel_resolution_grants g WHERE g.issuer=p_target_issuer AND g.subject=p_target_subject AND g.client_id=p_target_client AND g.environment_id=p_env AND g.decision=p_permission) END INTO v_granted;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('targetExists',v_exists,'granted',v_granted);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_resend_cancel_grant_read(text,text,text,text,bigint,text,text,text,text,text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_operator_jwt_resend_cancel_grant_change(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid,p_target_issuer text,p_target_subject text,p_target_client text,p_resource text,p_permission text,p_expected boolean,p_desired boolean,p_reason text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_resend_cancel_grant_requests%ROWTYPE; v_current boolean; v_changed boolean;
BEGIN
 IF p_request IS NULL OR NOT public.novu_rust_operator_jwt_resend_cancel_grant_input_valid(p_target_issuer,p_target_subject,p_target_client,p_resource,p_permission) OR p_expected IS NULL OR p_desired IS NULL OR p_reason IS NULL OR octet_length(btrim(p_reason)) NOT BETWEEN 1 AND 1000 THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_resend_cancel_grant_admin_access(p_issuer,p_subject,p_client,p_env,p_exp,true);
 IF (p_issuer,p_subject)=(p_target_issuer,p_target_subject) THEN RAISE EXCEPTION 'self change denied' USING ERRCODE='42501'; END IF;
 PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(pg_catalog.jsonb_build_array('jwt-resend-cancel-grant-request',p_issuer,p_subject,p_client,p_env,p_request)::text,0));
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 SELECT * INTO r FROM public.novu_rust_operator_jwt_resend_cancel_grant_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
 IF FOUND THEN
  IF (r.target_issuer,r.target_subject,r.target_client,r.resource,r.permission,r.expected,r.desired,r.reason) IS DISTINCT FROM (p_target_issuer,p_target_subject,p_target_client,p_resource,p_permission,p_expected,p_desired,p_reason) THEN RAISE EXCEPTION 'request conflict' USING ERRCODE='P4090'; END IF;
  IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
  RETURN jsonb_build_object('changed',r.changed,'replayed',true,'requestId',p_request);
 END IF;
 PERFORM 1 FROM public.novu_rust_operator_jwt_identities i WHERE i.issuer=p_target_issuer AND i.subject=p_target_subject AND i.client_id=p_target_client AND i.environment_id=p_env FOR UPDATE OF i;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF NOT FOUND THEN RAISE EXCEPTION 'target missing' USING ERRCODE='22023'; END IF;
 SELECT CASE p_permission WHEN 'submit' THEN EXISTS(SELECT 1 FROM public.novu_rust_operator_jwt_resend_cancel_submit_grants g WHERE g.issuer=p_target_issuer AND g.subject=p_target_subject AND g.client_id=p_target_client AND g.environment_id=p_env) WHEN 'list' THEN EXISTS(SELECT 1 FROM public.novu_rust_operator_jwt_resend_cancel_list_grants g WHERE g.issuer=p_target_issuer AND g.subject=p_target_subject AND g.client_id=p_target_client AND g.environment_id=p_env) WHEN 'detail' THEN EXISTS(SELECT 1 FROM public.novu_rust_operator_jwt_resend_cancel_detail_grants g WHERE g.issuer=p_target_issuer AND g.subject=p_target_subject AND g.client_id=p_target_client AND g.environment_id=p_env) ELSE EXISTS(SELECT 1 FROM public.novu_rust_operator_jwt_resend_cancel_resolution_grants g WHERE g.issuer=p_target_issuer AND g.subject=p_target_subject AND g.client_id=p_target_client AND g.environment_id=p_env AND g.decision=p_permission) END INTO v_current;
 IF v_current IS DISTINCT FROM p_expected THEN RAISE EXCEPTION 'stale grant' USING ERRCODE='P4090'; END IF;
 v_changed:=v_current IS DISTINCT FROM p_desired;
 IF v_changed THEN
  IF p_permission='submit' THEN
   IF p_desired THEN INSERT INTO public.novu_rust_operator_jwt_resend_cancel_submit_grants VALUES(p_target_issuer,p_target_subject,p_target_client,p_env) ON CONFLICT DO NOTHING;
   ELSE DELETE FROM public.novu_rust_operator_jwt_resend_cancel_submit_grants WHERE issuer=p_target_issuer AND subject=p_target_subject AND client_id=p_target_client AND environment_id=p_env; END IF;
  ELSIF p_permission='list' THEN
   IF p_desired THEN INSERT INTO public.novu_rust_operator_jwt_resend_cancel_list_grants VALUES(p_target_issuer,p_target_subject,p_target_client,p_env) ON CONFLICT DO NOTHING;
   ELSE DELETE FROM public.novu_rust_operator_jwt_resend_cancel_list_grants WHERE issuer=p_target_issuer AND subject=p_target_subject AND client_id=p_target_client AND environment_id=p_env; END IF;
  ELSIF p_permission='detail' THEN
   IF p_desired THEN INSERT INTO public.novu_rust_operator_jwt_resend_cancel_detail_grants VALUES(p_target_issuer,p_target_subject,p_target_client,p_env) ON CONFLICT DO NOTHING;
   ELSE DELETE FROM public.novu_rust_operator_jwt_resend_cancel_detail_grants WHERE issuer=p_target_issuer AND subject=p_target_subject AND client_id=p_target_client AND environment_id=p_env; END IF;
  ELSE
   IF p_desired THEN INSERT INTO public.novu_rust_operator_jwt_resend_cancel_resolution_grants VALUES(p_target_issuer,p_target_subject,p_target_client,p_env,p_permission) ON CONFLICT DO NOTHING;
   ELSE DELETE FROM public.novu_rust_operator_jwt_resend_cancel_resolution_grants WHERE issuer=p_target_issuer AND subject=p_target_subject AND client_id=p_target_client AND environment_id=p_env AND decision=p_permission; END IF;
  END IF;
  INSERT INTO public.novu_rust_operator_jwt_resend_cancel_grant_audit(environment_id,actor_issuer,actor_subject,actor_client,database_principal,request_id,target_issuer,target_subject,target_client,resource,permission,previous,current,reason) VALUES(p_env,p_issuer,p_subject,p_client,session_user,p_request,p_target_issuer,p_target_subject,p_target_client,p_resource,p_permission,p_expected,p_desired,p_reason);
 END IF;
 INSERT INTO public.novu_rust_operator_jwt_resend_cancel_grant_requests(issuer,subject,client_id,environment_id,request_id,target_issuer,target_subject,target_client,resource,permission,expected,desired,reason,changed) VALUES(p_issuer,p_subject,p_client,p_env,p_request,p_target_issuer,p_target_subject,p_target_client,p_resource,p_permission,p_expected,p_desired,p_reason,v_changed);
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('changed',v_changed,'replayed',false,'requestId',p_request);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_resend_cancel_grant_change(text,text,text,text,bigint,uuid,text,text,text,text,text,boolean,boolean,text) FROM PUBLIC;

CREATE TABLE public.novu_rust_operator_jwt_resend_cancel_command_audit (
 environment_id text NOT NULL,issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,jwt_request_id uuid NOT NULL,
 command_id uuid NOT NULL,job_id uuid NOT NULL,reason text NOT NULL,recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(environment_id,issuer,subject,client_id,jwt_request_id),UNIQUE(environment_id,command_id),
 FOREIGN KEY(environment_id,command_id) REFERENCES public.novu_rust_resend_cancel_commands(environment_id,request_id)
);
REVOKE ALL ON public.novu_rust_operator_jwt_resend_cancel_command_audit FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_resend_cancel_submit_access(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_resend_cancel_submit_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 PERFORM 1 FROM public.novu_rust_resend_cancel_submitters g JOIN pg_catalog.pg_roles r ON r.oid=g.principal_oid AND r.rolname=g.database_principal AND r.rolcanlogin WHERE g.environment_id=p_env AND g.database_principal=session_user FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_resend_cancel_submit_access(text,text,text,text,bigint) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_resend_cancel_submit(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid,p_job uuid,p_reason text) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_old public.novu_rust_operator_jwt_resend_cancel_command_audit%ROWTYPE;v_id uuid;v_result jsonb;
BEGIN
 IF p_request IS NULL OR p_job IS NULL OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>500 OR p_reason ~ '[[:cntrl:]]' THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_resend_cancel_submit_access(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(pg_catalog.jsonb_build_array('resend-cancel-submit',p_env,p_issuer,p_subject,p_client,p_request)::text,0));
 SELECT * INTO v_old FROM public.novu_rust_operator_jwt_resend_cancel_command_audit WHERE environment_id=p_env AND issuer=p_issuer AND subject=p_subject AND client_id=p_client AND jwt_request_id=p_request;
 IF FOUND THEN
  IF (v_old.job_id,v_old.reason) IS DISTINCT FROM (p_job,p_reason) THEN RAISE EXCEPTION 'request conflict' USING ERRCODE='P4090'; END IF;
  v_result:=public.novu_rust_resend_cancel_command_get(p_env,v_old.command_id);
  IF v_result IS NULL THEN RAISE EXCEPTION 'command unavailable' USING ERRCODE='55000'; END IF;
  IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
  RETURN jsonb_build_object('requestId',p_request,'command',v_result,'replayed',true);
 END IF;
 v_id:=substr(encode(sha256(convert_to(jsonb_build_array('resend-cancel-command-v1',p_env,p_issuer,p_subject,p_client,p_request)::text,'UTF8')),'hex'),1,32)::uuid;
 PERFORM public.novu_rust_resend_cancel_enqueue(p_env,v_id,p_job,p_reason);
 INSERT INTO public.novu_rust_operator_jwt_resend_cancel_command_audit(environment_id,issuer,subject,client_id,jwt_request_id,command_id,job_id,reason) VALUES(p_env,p_issuer,p_subject,p_client,p_request,v_id,p_job,p_reason);
 v_result:=public.novu_rust_resend_cancel_command_get(p_env,v_id);
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('requestId',p_request,'command',v_result,'replayed',false);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_resend_cancel_submit(text,text,text,text,bigint,uuid,uuid,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_resend_cancel_command_get(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_id uuid;v_command jsonb;
BEGIN
 IF p_request IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_resend_cancel_submit_access(p_issuer,p_subject,p_client,p_env,p_exp);
 SELECT command_id INTO v_id FROM public.novu_rust_operator_jwt_resend_cancel_command_audit WHERE environment_id=p_env AND issuer=p_issuer AND subject=p_subject AND client_id=p_client AND jwt_request_id=p_request;
 IF FOUND THEN v_command:=public.novu_rust_resend_cancel_command_get(p_env,v_id); END IF;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('requestId',p_request,'command',v_command);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_resend_cancel_command_get(text,text,text,text,bigint,uuid) FROM PUBLIC;
