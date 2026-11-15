-- Optional resource-scoped delegation, independent of broad administrators.
CREATE TABLE public.novu_rust_operator_jwt_channel_access_grant_scopes (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,
 resource text NOT NULL CHECK(resource IN ('resend','sendgrid','twilio','fcm','slack')),
 can_read boolean NOT NULL DEFAULT false,can_write boolean NOT NULL DEFAULT false,
 PRIMARY KEY(issuer,subject,client_id,environment_id,resource),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
REVOKE ALL ON public.novu_rust_operator_jwt_channel_access_grant_scopes FROM PUBLIC;

CREATE FUNCTION public.novu_rust_operator_jwt_channel_access_scope_check(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_write boolean,p_resource text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_write IS NULL OR (p_resource IS NOT NULL AND p_resource NOT IN ('resend','sendgrid','twilio','fcm','slack')) THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_channel_access_grant_admins g
  WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env AND CASE WHEN p_write THEN g.can_write ELSE g.can_read END FOR SHARE OF g;
 IF NOT FOUND THEN
  PERFORM 1 FROM public.novu_rust_operator_jwt_channel_access_grant_scopes g
   WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env AND (p_resource IS NULL OR g.resource=p_resource)
    AND CASE WHEN p_write THEN g.can_write ELSE g.can_read END ORDER BY g.resource LIMIT 1 FOR SHARE OF g;
  IF NOT FOUND THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 END IF;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_channel_access_scope_check(text,text,text,text,bigint,boolean,text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_operator_jwt_channel_access_grant_read(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_target_issuer text,p_target_subject text,p_target_client text,p_resource text,p_permission text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_exists boolean; v_granted boolean;
BEGIN
 IF NOT public.novu_rust_operator_jwt_channel_access_grant_input_valid(p_target_issuer,p_target_subject,p_target_client,p_resource,p_permission) THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_channel_access_scope_check(p_issuer,p_subject,p_client,p_env,p_exp,false,p_resource);
 PERFORM 1 FROM public.novu_rust_operator_jwt_identities i WHERE i.issuer=p_target_issuer AND i.subject=p_target_subject AND i.client_id=p_target_client AND i.environment_id=p_env FOR SHARE OF i;
 v_exists:=FOUND;
 EXECUTE format('SELECT EXISTS(SELECT 1 FROM public.%I WHERE issuer=$1 AND subject=$2 AND client_id=$3 AND environment_id=$4)', 'novu_rust_operator_jwt_'||p_resource||'_'||p_permission||'_grants') INTO v_granted USING p_target_issuer,p_target_subject,p_target_client,p_env;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('targetExists',v_exists,'granted',v_granted);
END; $$;

CREATE OR REPLACE FUNCTION public.novu_rust_operator_jwt_channel_access_grant_change(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid,p_target_issuer text,p_target_subject text,p_target_client text,p_resource text,p_permission text,p_expected boolean,p_desired boolean,p_reason text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_channel_access_grant_requests%ROWTYPE; v_current boolean; v_changed boolean;
BEGIN
 IF p_request IS NULL OR NOT public.novu_rust_operator_jwt_channel_access_grant_input_valid(p_target_issuer,p_target_subject,p_target_client,p_resource,p_permission) OR p_expected IS NULL OR p_desired IS NULL OR p_reason IS NULL OR octet_length(btrim(p_reason)) NOT BETWEEN 1 AND 1000 THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_channel_access_scope_check(p_issuer,p_subject,p_client,p_env,p_exp,true,p_resource);
 IF (p_issuer,p_subject)=(p_target_issuer,p_target_subject) THEN RAISE EXCEPTION 'self change denied' USING ERRCODE='42501'; END IF;
 PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(pg_catalog.jsonb_build_array('jwt-channel-access-grant-request',p_issuer,p_subject,p_client,p_env,p_request)::text,0));
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 SELECT * INTO r FROM public.novu_rust_operator_jwt_channel_access_grant_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
 IF FOUND THEN
  IF (r.target_issuer,r.target_subject,r.target_client,r.resource,r.permission,r.expected,r.desired,r.reason) IS DISTINCT FROM (p_target_issuer,p_target_subject,p_target_client,p_resource,p_permission,p_expected,p_desired,p_reason) THEN RAISE EXCEPTION 'request conflict' USING ERRCODE='P4090'; END IF;
  IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
  RETURN jsonb_build_object('changed',r.changed,'replayed',true,'requestId',p_request);
 END IF;
 PERFORM 1 FROM public.novu_rust_operator_jwt_identities i WHERE i.issuer=p_target_issuer AND i.subject=p_target_subject AND i.client_id=p_target_client AND i.environment_id=p_env FOR UPDATE OF i;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF NOT FOUND THEN RAISE EXCEPTION 'target missing' USING ERRCODE='22023'; END IF;
 EXECUTE format('SELECT EXISTS(SELECT 1 FROM public.%I WHERE issuer=$1 AND subject=$2 AND client_id=$3 AND environment_id=$4)', 'novu_rust_operator_jwt_'||p_resource||'_'||p_permission||'_grants') INTO v_current USING p_target_issuer,p_target_subject,p_target_client,p_env;
 IF v_current IS DISTINCT FROM p_expected THEN RAISE EXCEPTION 'stale grant' USING ERRCODE='P4090'; END IF;
 v_changed:=v_current IS DISTINCT FROM p_desired;
 IF v_changed THEN
  IF p_desired THEN
   EXECUTE format('INSERT INTO public.%I(issuer,subject,client_id,environment_id) VALUES($1,$2,$3,$4)', 'novu_rust_operator_jwt_'||p_resource||'_'||p_permission||'_grants') USING p_target_issuer,p_target_subject,p_target_client,p_env;
  ELSE
   EXECUTE format('DELETE FROM public.%I WHERE issuer=$1 AND subject=$2 AND client_id=$3 AND environment_id=$4', 'novu_rust_operator_jwt_'||p_resource||'_'||p_permission||'_grants') USING p_target_issuer,p_target_subject,p_target_client,p_env;
  END IF;
  INSERT INTO public.novu_rust_operator_jwt_channel_access_grant_audit(environment_id,actor_issuer,actor_subject,actor_client,database_principal,request_id,target_issuer,target_subject,target_client,resource,permission,previous,current,reason) VALUES(p_env,p_issuer,p_subject,p_client,session_user,p_request,p_target_issuer,p_target_subject,p_target_client,p_resource,p_permission,p_expected,p_desired,p_reason);
 END IF;
 INSERT INTO public.novu_rust_operator_jwt_channel_access_grant_requests(issuer,subject,client_id,environment_id,request_id,target_issuer,target_subject,target_client,resource,permission,expected,desired,reason,changed) VALUES(p_issuer,p_subject,p_client,p_env,p_request,p_target_issuer,p_target_subject,p_target_client,p_resource,p_permission,p_expected,p_desired,p_reason,v_changed);
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('changed',v_changed,'replayed',false,'requestId',p_request);
END; $$;

CREATE OR REPLACE FUNCTION public.novu_rust_operator_jwt_channel_access_grant_request(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_channel_access_grant_requests%ROWTYPE;
BEGIN
 IF p_request IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_channel_access_scope_check(p_issuer,p_subject,p_client,p_env,p_exp,true,NULL);
 SELECT * INTO r FROM public.novu_rust_operator_jwt_channel_access_grant_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF NOT FOUND THEN RETURN jsonb_build_object('found',false); END IF;
 PERFORM public.novu_rust_operator_jwt_channel_access_scope_check(p_issuer,p_subject,p_client,p_env,p_exp,true,r.resource);
 RETURN jsonb_build_object('found',true,'requestId',r.request_id,'targetIssuer',r.target_issuer,'targetSubject',r.target_subject,'targetClientId',r.target_client,'resource',r.resource,'permission',r.permission,'expected',r.expected,'desired',r.desired,'reason',r.reason,'changed',r.changed,'createdAt',r.created_at);
END; $$;
