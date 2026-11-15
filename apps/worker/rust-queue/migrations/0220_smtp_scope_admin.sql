-- Environment-scoped delegated administration. Trusted DBA provisions these grants.
CREATE TABLE public.novu_rust_smtp_scope_admins (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),
 database_principal name NOT NULL, principal_oid oid NOT NULL,
 can_read boolean NOT NULL DEFAULT false, can_write boolean NOT NULL DEFAULT false,
 PRIMARY KEY(environment_id,database_principal)
);
CREATE TABLE public.novu_rust_smtp_scope_admin_requests (
 environment_id text NOT NULL,actor name NOT NULL,actor_oid oid NOT NULL,request_id uuid NOT NULL,
 target name NOT NULL,target_oid oid NOT NULL,expected text NOT NULL,desired boolean NOT NULL,
 reason text NOT NULL,result jsonb,recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(environment_id,actor_oid,request_id)
);
REVOKE ALL ON public.novu_rust_smtp_scope_admins,public.novu_rust_smtp_scope_admin_requests FROM PUBLIC;
CREATE FUNCTION public.novu_rust_smtp_scope_admin_access(p_env text,p_write boolean) RETURNS oid
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_oid oid;
BEGIN
 IF p_env IS NULL OR length(btrim(p_env))=0 OR p_write IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 SELECT r.oid INTO v_oid FROM public.novu_rust_smtp_scope_admins a JOIN pg_catalog.pg_roles r ON r.oid=a.principal_oid AND r.rolname=a.database_principal AND r.rolcanlogin
 WHERE a.environment_id=p_env AND a.database_principal=session_user AND CASE WHEN p_write THEN a.can_write ELSE a.can_read END FOR SHARE OF a;
 IF NOT FOUND THEN RAISE EXCEPTION 'scope administration denied' USING ERRCODE='42501'; END IF;
 RETURN v_oid;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_smtp_scope_admin_access(text,boolean) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_smtp_scope_admin_read(p_env text,p_target text) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_oid oid;v_scope public.novu_rust_smtp_scoped_logins%ROWTYPE;
BEGIN
 IF p_target IS NULL OR octet_length(p_target) NOT BETWEEN 1 AND 63 THEN RAISE EXCEPTION 'invalid target' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_smtp_scope_admin_access(p_env,false);
 SELECT oid INTO v_oid FROM pg_catalog.pg_roles WHERE rolname=p_target AND rolcanlogin;
 IF NOT FOUND THEN RETURN jsonb_build_object('available',false); END IF;
 SELECT * INTO v_scope FROM public.novu_rust_smtp_scoped_logins WHERE principal_oid=v_oid;
 IF FOUND AND (v_scope.environment_id<>p_env OR v_scope.database_principal::text<>p_target) THEN RETURN jsonb_build_object('available',false); END IF;
 RETURN jsonb_build_object('available',true,'state',CASE WHEN v_scope.principal_oid IS NULL THEN 'absent' WHEN v_scope.enabled THEN 'enabled' ELSE 'disabled' END);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_smtp_scope_admin_read(text,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_smtp_scope_admin_set(p_env text,p_request uuid,p_target text,p_expected text,p_desired boolean,p_reason text) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_actor oid;v_target oid;v_added boolean;v_req public.novu_rust_smtp_scope_admin_requests%ROWTYPE;v_scope public.novu_rust_smtp_scoped_logins%ROWTYPE;v_state text;v_result jsonb;
BEGIN
 IF p_request IS NULL OR p_target IS NULL OR octet_length(p_target) NOT BETWEEN 1 AND 63 OR p_expected IS NULL OR p_expected NOT IN ('absent','enabled','disabled') OR p_desired IS NULL OR p_reason IS NULL OR octet_length(btrim(p_reason)) NOT BETWEEN 1 AND 512 THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 v_actor:=public.novu_rust_smtp_scope_admin_access(p_env,true);
 IF p_target=session_user THEN RAISE EXCEPTION 'self change denied' USING ERRCODE='42501'; END IF;
 SELECT oid INTO v_target FROM pg_catalog.pg_roles WHERE rolname=p_target AND rolcanlogin;
 IF NOT FOUND THEN RAISE EXCEPTION 'target unavailable' USING ERRCODE='22023'; END IF;
 INSERT INTO public.novu_rust_smtp_scope_admin_requests(environment_id,actor,actor_oid,request_id,target,target_oid,expected,desired,reason)
 VALUES(p_env,session_user,v_actor,p_request,p_target::name,v_target,p_expected,p_desired,p_reason) ON CONFLICT DO NOTHING RETURNING true INTO v_added;
 IF NOT coalesce(v_added,false) THEN
  SELECT * INTO v_req FROM public.novu_rust_smtp_scope_admin_requests WHERE environment_id=p_env AND actor_oid=v_actor AND request_id=p_request;
  IF NOT FOUND OR (v_req.actor,v_req.target,v_req.target_oid,v_req.expected,v_req.desired,v_req.reason) IS DISTINCT FROM (session_user,p_target::name,v_target,p_expected,p_desired,p_reason) OR v_req.result IS NULL THEN RAISE EXCEPTION 'request conflict' USING ERRCODE='23505'; END IF;
  RETURN jsonb_build_object('requestId',p_request,'replayed',true,'result',v_req.result);
 END IF;
 PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('smtp-scope-target:'||v_target::text,0));
 SELECT * INTO v_scope FROM public.novu_rust_smtp_scoped_logins WHERE principal_oid=v_target FOR UPDATE;
 IF FOUND AND (v_scope.environment_id<>p_env OR v_scope.database_principal::text<>p_target) THEN RAISE EXCEPTION 'target unavailable' USING ERRCODE='22023'; END IF;
 v_state:=CASE WHEN v_scope.principal_oid IS NULL THEN 'absent' WHEN v_scope.enabled THEN 'enabled' ELSE 'disabled' END;
 IF v_state<>p_expected THEN RAISE EXCEPTION 'scope changed' USING ERRCODE='40001'; END IF;
 IF (SELECT count(*) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind='r' AND c.relname=ANY(ARRAY['novu_queue_jobs','novu_rust_smtp_attempts','novu_rust_email_workflow_worker_config','novu_rust_subscribers','novu_rust_workflow_preferences','novu_rust_subscriber_preferences','novu_rust_subscriber_workflow_preferences','novu_rust_workflows','novu_rust_contexts','novu_rust_email_messages','novu_rust_email_step_outcomes']))<>11 OR EXISTS(
  SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
  WHERE n.nspname='public' AND c.relname=ANY(ARRAY['novu_queue_jobs','novu_rust_smtp_attempts','novu_rust_email_workflow_worker_config','novu_rust_subscribers','novu_rust_workflow_preferences','novu_rust_subscriber_preferences','novu_rust_subscriber_workflow_preferences','novu_rust_workflows','novu_rust_contexts','novu_rust_email_messages','novu_rust_email_step_outcomes'])
  AND (NOT c.relrowsecurity OR NOT EXISTS(SELECT 1 FROM pg_catalog.pg_policy p WHERE p.polrelid=c.oid AND p.polname='swallow_smtp_baseline' AND p.polpermissive) OR NOT EXISTS(SELECT 1 FROM pg_catalog.pg_policy p WHERE p.polrelid=c.oid AND p.polname='swallow_smtp_restrict' AND NOT p.polpermissive))
 ) THEN RAISE EXCEPTION 'scope isolation unavailable' USING ERRCODE='42501'; END IF;
 IF p_desired AND (EXISTS(SELECT 1 FROM pg_catalog.pg_roles privileged WHERE (privileged.rolsuper OR privileged.rolbypassrls) AND pg_catalog.pg_has_role(v_target,privileged.oid,'MEMBER')) OR EXISTS(
  SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
  WHERE n.nspname='public' AND c.relname=ANY(ARRAY['novu_queue_jobs','novu_rust_smtp_attempts','novu_rust_email_workflow_worker_config','novu_rust_subscribers','novu_rust_workflow_preferences','novu_rust_subscriber_preferences','novu_rust_subscriber_workflow_preferences','novu_rust_workflows','novu_rust_contexts','novu_rust_email_messages','novu_rust_email_step_outcomes'])
  AND pg_catalog.pg_has_role(v_target,c.relowner,'MEMBER')
 )) THEN RAISE EXCEPTION 'target isolation unavailable' USING ERRCODE='42501'; END IF;
 IF v_state='absent' THEN
  INSERT INTO public.novu_rust_smtp_scoped_logins(principal_oid,database_principal,environment_id,enabled) VALUES(v_target,p_target::name,p_env,p_desired);
 ELSE
  UPDATE public.novu_rust_smtp_scoped_logins SET enabled=p_desired WHERE principal_oid=v_target;
 END IF;
 v_result:=jsonb_build_object('previous',v_state,'state',CASE WHEN p_desired THEN 'enabled' ELSE 'disabled' END,'changed',v_state='absent' OR (v_state='enabled')<>p_desired);
 UPDATE public.novu_rust_smtp_scope_admin_requests SET result=v_result WHERE environment_id=p_env AND actor_oid=v_actor AND request_id=p_request;
 RETURN jsonb_build_object('requestId',p_request,'replayed',false,'result',v_result);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_smtp_scope_admin_set(text,uuid,text,text,boolean,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_smtp_scope_admin_request(p_env text,p_request uuid) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_actor oid;v_req public.novu_rust_smtp_scope_admin_requests%ROWTYPE;
BEGIN
 IF p_request IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 v_actor:=public.novu_rust_smtp_scope_admin_access(p_env,true);
 SELECT * INTO v_req FROM public.novu_rust_smtp_scope_admin_requests WHERE environment_id=p_env AND actor_oid=v_actor AND request_id=p_request AND actor=session_user;
 IF NOT FOUND THEN RETURN jsonb_build_object('found',false); END IF;
 RETURN jsonb_build_object('found',true,'requestId',v_req.request_id,'target',v_req.target,'expected',v_req.expected,'desired',v_req.desired,'reason',v_req.reason,'result',v_req.result,'recordedAt',v_req.recorded_at::text);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_smtp_scope_admin_request(text,uuid) FROM PUBLIC;
