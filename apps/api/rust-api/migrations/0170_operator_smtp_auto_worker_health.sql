-- Independent JWT permission; the API DB login also needs alert_view.
CREATE TABLE public.novu_rust_operator_jwt_smtp_auto_worker_grants (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,
 can_health boolean NOT NULL DEFAULT false,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
REVOKE ALL ON public.novu_rust_operator_jwt_smtp_auto_worker_grants FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_smtp_auto_worker_health(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_max_age integer,p_minimum integer)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE permitted boolean;result jsonb;
BEGIN
 IF p_max_age IS NULL OR p_max_age NOT BETWEEN 1 AND 86400 OR p_minimum IS NULL OR p_minimum NOT BETWEEN 1 AND 1000 THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 SELECT can_health INTO permitted FROM public.novu_rust_operator_jwt_smtp_auto_worker_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT coalesce(permitted,false) OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 SELECT jsonb_build_object('healthy',healthy,'stale',stale,'minimumHealthy',p_minimum,'needsAttention',healthy<p_minimum) INTO result FROM public.novu_rust_smtp_auto_worker_health_check(p_env,p_max_age);
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_smtp_auto_worker_health(text,text,text,text,bigint,integer,integer) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_operator_jwt_smtp_auto_grant_input_valid(p_issuer text,p_subject text,p_client text,p_resource text,p_permission text)
RETURNS boolean LANGUAGE sql IMMUTABLE SET search_path=pg_catalog AS $$
 SELECT COALESCE(octet_length(p_issuer) BETWEEN 1 AND 2048 AND octet_length(p_subject) BETWEEN 1 AND 1024 AND octet_length(p_client) BETWEEN 1 AND 1024 AND (p_resource='smtp-auto' AND p_permission IN ('read','write','audit','history','health','worker-health')),false)
$$;

CREATE OR REPLACE FUNCTION public.novu_rust_operator_jwt_smtp_auto_grant_read(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_target_issuer text,p_target_subject text,p_target_client text,p_resource text,p_permission text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_exists boolean; v_granted boolean;
BEGIN
 IF NOT public.novu_rust_operator_jwt_smtp_auto_grant_input_valid(p_target_issuer,p_target_subject,p_target_client,p_resource,p_permission) THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_smtp_auto_grant_admin_access(p_issuer,p_subject,p_client,p_env,p_exp,false);
 PERFORM 1 FROM public.novu_rust_operator_jwt_identities i WHERE i.issuer=p_target_issuer AND i.subject=p_target_subject AND i.client_id=p_target_client AND i.environment_id=p_env FOR SHARE OF i;
 v_exists:=FOUND;
 SELECT CASE WHEN p_permission='worker-health' THEN COALESCE((SELECT g.can_health FROM public.novu_rust_operator_jwt_smtp_auto_worker_grants g WHERE g.issuer=p_target_issuer AND g.subject=p_target_subject AND g.client_id=p_target_client AND g.environment_id=p_env),false) WHEN p_permission='health' THEN COALESCE((SELECT g.can_health FROM public.novu_rust_operator_jwt_smtp_auto_health_grants g WHERE g.issuer=p_target_issuer AND g.subject=p_target_subject AND g.client_id=p_target_client AND g.environment_id=p_env),false) ELSE COALESCE((SELECT CASE p_permission WHEN 'read' THEN g.can_read WHEN 'write' THEN g.can_write WHEN 'audit' THEN g.can_audit ELSE g.can_history END FROM public.novu_rust_operator_jwt_smtp_auto_policy_grants g WHERE g.issuer=p_target_issuer AND g.subject=p_target_subject AND g.client_id=p_target_client AND g.environment_id=p_env),false) END INTO v_granted;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('targetExists',v_exists,'granted',v_granted);
END; $$;

CREATE OR REPLACE FUNCTION public.novu_rust_operator_jwt_smtp_auto_grant_change(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid,p_target_issuer text,p_target_subject text,p_target_client text,p_resource text,p_permission text,p_expected boolean,p_desired boolean,p_reason text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_smtp_auto_grant_requests%ROWTYPE; v_current boolean; v_changed boolean;
BEGIN
 IF p_request IS NULL OR NOT public.novu_rust_operator_jwt_smtp_auto_grant_input_valid(p_target_issuer,p_target_subject,p_target_client,p_resource,p_permission) OR p_expected IS NULL OR p_desired IS NULL OR p_reason IS NULL OR octet_length(btrim(p_reason)) NOT BETWEEN 1 AND 1000 THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_smtp_auto_grant_admin_access(p_issuer,p_subject,p_client,p_env,p_exp,true);
 IF (p_issuer,p_subject)=(p_target_issuer,p_target_subject) THEN RAISE EXCEPTION 'self change denied' USING ERRCODE='42501'; END IF;
 PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(pg_catalog.jsonb_build_array('jwt-smtp-auto-grant-request',p_issuer,p_subject,p_client,p_env,p_request)::text,0));
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 SELECT * INTO r FROM public.novu_rust_operator_jwt_smtp_auto_grant_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
 IF FOUND THEN
  IF (r.target_issuer,r.target_subject,r.target_client,r.resource,r.permission,r.expected,r.desired,r.reason) IS DISTINCT FROM (p_target_issuer,p_target_subject,p_target_client,p_resource,p_permission,p_expected,p_desired,p_reason) THEN RAISE EXCEPTION 'request conflict' USING ERRCODE='P4090'; END IF;
  IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
  RETURN jsonb_build_object('changed',r.changed,'replayed',true,'requestId',p_request);
 END IF;
 PERFORM 1 FROM public.novu_rust_operator_jwt_identities i WHERE i.issuer=p_target_issuer AND i.subject=p_target_subject AND i.client_id=p_target_client AND i.environment_id=p_env FOR UPDATE OF i;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF NOT FOUND THEN RAISE EXCEPTION 'target missing' USING ERRCODE='22023'; END IF;
 SELECT CASE WHEN p_permission='worker-health' THEN COALESCE((SELECT g.can_health FROM public.novu_rust_operator_jwt_smtp_auto_worker_grants g WHERE g.issuer=p_target_issuer AND g.subject=p_target_subject AND g.client_id=p_target_client AND g.environment_id=p_env),false) WHEN p_permission='health' THEN COALESCE((SELECT g.can_health FROM public.novu_rust_operator_jwt_smtp_auto_health_grants g WHERE g.issuer=p_target_issuer AND g.subject=p_target_subject AND g.client_id=p_target_client AND g.environment_id=p_env),false) ELSE COALESCE((SELECT CASE p_permission WHEN 'read' THEN g.can_read WHEN 'write' THEN g.can_write WHEN 'audit' THEN g.can_audit ELSE g.can_history END FROM public.novu_rust_operator_jwt_smtp_auto_policy_grants g WHERE g.issuer=p_target_issuer AND g.subject=p_target_subject AND g.client_id=p_target_client AND g.environment_id=p_env),false) END INTO v_current;
 IF v_current IS DISTINCT FROM p_expected THEN RAISE EXCEPTION 'stale grant' USING ERRCODE='P4090'; END IF;
 v_changed:=v_current IS DISTINCT FROM p_desired;
 IF v_changed THEN
  IF p_permission='worker-health' THEN
   INSERT INTO public.novu_rust_operator_jwt_smtp_auto_worker_grants(issuer,subject,client_id,environment_id,can_health) VALUES(p_target_issuer,p_target_subject,p_target_client,p_env,p_desired) ON CONFLICT(issuer,subject,client_id,environment_id) DO UPDATE SET can_health=p_desired;
  ELSIF p_permission='health' THEN
   INSERT INTO public.novu_rust_operator_jwt_smtp_auto_health_grants(issuer,subject,client_id,environment_id,can_health) VALUES(p_target_issuer,p_target_subject,p_target_client,p_env,p_desired) ON CONFLICT(issuer,subject,client_id,environment_id) DO UPDATE SET can_health=p_desired;
  ELSE
  INSERT INTO public.novu_rust_operator_jwt_smtp_auto_policy_grants(issuer,subject,client_id,environment_id,can_read,can_write,can_audit,can_history)
  VALUES(p_target_issuer,p_target_subject,p_target_client,p_env,p_permission='read' AND p_desired,p_permission='write' AND p_desired,p_permission='audit' AND p_desired,p_permission='history' AND p_desired)
  ON CONFLICT(issuer,subject,client_id,environment_id) DO UPDATE SET
    can_read=CASE WHEN p_permission='read' THEN p_desired ELSE public.novu_rust_operator_jwt_smtp_auto_policy_grants.can_read END,
    can_write=CASE WHEN p_permission='write' THEN p_desired ELSE public.novu_rust_operator_jwt_smtp_auto_policy_grants.can_write END,
    can_audit=CASE WHEN p_permission='audit' THEN p_desired ELSE public.novu_rust_operator_jwt_smtp_auto_policy_grants.can_audit END,
    can_history=CASE WHEN p_permission='history' THEN p_desired ELSE public.novu_rust_operator_jwt_smtp_auto_policy_grants.can_history END;
  END IF;
  INSERT INTO public.novu_rust_operator_jwt_smtp_auto_grant_audit(environment_id,actor_issuer,actor_subject,actor_client,database_principal,request_id,target_issuer,target_subject,target_client,resource,permission,previous,current,reason) VALUES(p_env,p_issuer,p_subject,p_client,session_user,p_request,p_target_issuer,p_target_subject,p_target_client,p_resource,p_permission,p_expected,p_desired,p_reason);
 END IF;
 INSERT INTO public.novu_rust_operator_jwt_smtp_auto_grant_requests(issuer,subject,client_id,environment_id,request_id,target_issuer,target_subject,target_client,resource,permission,expected,desired,reason,changed) VALUES(p_issuer,p_subject,p_client,p_env,p_request,p_target_issuer,p_target_subject,p_target_client,p_resource,p_permission,p_expected,p_desired,p_reason,v_changed);
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('changed',v_changed,'replayed',false,'requestId',p_request);
END; $$;
